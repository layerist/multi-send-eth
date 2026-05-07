#!/usr/bin/env python3
from __future__ import annotations

import concurrent.futures
import json
import logging
import os
import random
import signal
import threading
import time
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from web3 import Web3
from web3.exceptions import (
    TimeExhausted,
    TransactionNotFound,
)
from web3.providers.rpc import HTTPProvider


# =============================================================================
# CONFIG
# =============================================================================

@dataclass(frozen=True)
class Config:
    # Network
    chain_id: int = int(os.getenv("CHAIN_ID", "1"))
    network: str = os.getenv("NETWORK", "mainnet")
    infura_project_id: Optional[str] = os.getenv("INFURA_PROJECT_ID")

    # Concurrency
    max_workers: int = int(os.getenv("MAX_WORKERS", "20"))

    # Sending
    max_send_retries: int = int(os.getenv("MAX_SEND_RETRIES", "6"))
    replacement_bump_percent: int = int(
        os.getenv("REPLACEMENT_BUMP_PERCENT", "15")
    )
    min_bump_wei: int = int(
        os.getenv("MIN_BUMP_WEI", str(1_000_000_000))
    )  # 1 gwei

    # Fees
    max_priority_fee_gwei: float = float(
        os.getenv("MAX_PRIORITY_FEE_GWEI", "2")
    )
    max_fee_multiplier: float = float(
        os.getenv("MAX_FEE_MULTIPLIER", "2.0")
    )

    # Gas
    gas_limit_fallback: int = int(
        os.getenv("GAS_LIMIT_FALLBACK", "21000")
    )
    gas_safety_multiplier: float = float(
        os.getenv("GAS_SAFETY_MULTIPLIER", "1.15")
    )

    # Receipt
    receipt_timeout: int = int(os.getenv("RECEIPT_TIMEOUT", "180"))
    receipt_poll: float = float(os.getenv("RECEIPT_POLL", "2"))
    confirmations: int = int(os.getenv("CONFIRMATIONS", "2"))

    # Files
    wallets_file: str = os.getenv("WALLETS_FILE", "wallets.json")
    failed_file: str = os.getenv("FAILED_FILE", "failed.json")
    success_file: str = os.getenv("SUCCESS_FILE", "success.json")

    # Runtime
    rpc_timeout: int = int(os.getenv("RPC_TIMEOUT", "30"))
    dry_run: bool = bool(int(os.getenv("DRY_RUN", "0")))

    # Performance
    session_pool_size: int = int(os.getenv("SESSION_POOL_SIZE", "100"))

    # Safety
    skip_zero_balance: bool = bool(
        int(os.getenv("SKIP_ZERO_BALANCE", "1"))
    )


CONFIG = Config()


# =============================================================================
# GLOBALS
# =============================================================================

STOP = threading.Event()

FAILED_LOCK = threading.Lock()
SUCCESS_LOCK = threading.Lock()
NONCE_LOCK = threading.Lock()

_web3: Optional[Web3] = None
_cached_chain_id: Optional[int] = None


# =============================================================================
# LOGGING
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

logger = logging.getLogger("sender")


# =============================================================================
# SESSION
# =============================================================================

def create_session() -> requests.Session:
    session = requests.Session()

    retry = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=[
            429,
            500,
            502,
            503,
            504,
        ],
        allowed_methods=False,
    )

    adapter = HTTPAdapter(
        pool_connections=CONFIG.session_pool_size,
        pool_maxsize=CONFIG.session_pool_size,
        max_retries=retry,
    )

    session.mount("http://", adapter)
    session.mount("https://", adapter)

    return session


HTTP_SESSION = create_session()


# =============================================================================
# WEB3 SINGLETON
# =============================================================================

def w3() -> Web3:
    global _web3
    global _cached_chain_id

    if _web3 is not None:
        return _web3

    with NONCE_LOCK:
        if _web3 is not None:
            return _web3

        if not CONFIG.infura_project_id:
            raise RuntimeError(
                "INFURA_PROJECT_ID environment variable missing"
            )

        rpc_url = (
            f"https://{CONFIG.network}.infura.io/v3/"
            f"{CONFIG.infura_project_id}"
        )

        provider = HTTPProvider(
            rpc_url,
            request_kwargs={
                "timeout": CONFIG.rpc_timeout,
                "session": HTTP_SESSION,
            },
        )

        client = Web3(provider)

        if not client.is_connected():
            raise ConnectionError("Unable to connect to Ethereum RPC")

        chain_id = client.eth.chain_id

        if chain_id != CONFIG.chain_id:
            raise RuntimeError(
                f"Wrong chain connected "
                f"(expected={CONFIG.chain_id}, got={chain_id})"
            )

        _cached_chain_id = chain_id
        _web3 = client

        logger.info(
            f"Connected to Ethereum "
            f"(chain_id={chain_id}, network={CONFIG.network})"
        )

        return client


# =============================================================================
# HELPERS
# =============================================================================

def backoff(attempt: int) -> float:
    base = min(15, 2 ** attempt)
    jitter = random.uniform(0.2, 1.0)
    return base + jitter


def safe_json_load(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []

    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []


def atomic_write_json(
    path: Path,
    data: List[Dict[str, Any]],
) -> None:
    temp = path.with_suffix(".tmp")

    with temp.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False, default=str)

    temp.replace(path)


def atomic_append(
    path: Path,
    item: Dict[str, Any],
    lock: threading.Lock,
) -> None:
    with lock:
        existing = safe_json_load(path)
        existing.append(item)
        atomic_write_json(path, existing)


def short(addr: str) -> str:
    return f"{addr[:6]}...{addr[-4:]}"


# =============================================================================
# NONCE MANAGER
# =============================================================================

class NonceManager:
    """
    Thread-safe nonce allocator.
    Prevents nonce collisions between worker threads.
    """

    def __init__(self):
        self.lock = threading.Lock()
        self.nonces: Dict[str, int] = {}

    def get(self, address: str) -> int:
        address = Web3.to_checksum_address(address)

        with self.lock:
            chain_nonce = w3().eth.get_transaction_count(
                address,
                "pending",
            )

            local_nonce = self.nonces.get(address)

            if local_nonce is None:
                nonce = chain_nonce
            else:
                nonce = max(chain_nonce, local_nonce)

            self.nonces[address] = nonce + 1

            return nonce

    def sync(self, address: str) -> int:
        address = Web3.to_checksum_address(address)

        with self.lock:
            nonce = w3().eth.get_transaction_count(
                address,
                "pending",
            )

            self.nonces[address] = nonce

            return nonce


NONCE = NonceManager()


# =============================================================================
# FEE LOGIC
# =============================================================================

def get_fees(
    previous: Optional[Dict[str, int]] = None,
) -> Dict[str, int]:
    client = w3()

    latest_block = client.eth.get_block("latest")

    base_fee = latest_block.get("baseFeePerGas")

    if base_fee is None:
        gas_price = client.eth.gas_price

        return {
            "gasPrice": int(gas_price * CONFIG.max_fee_multiplier)
        }

    if previous:
        bump = 1 + (CONFIG.replacement_bump_percent / 100)

        priority = max(
            int(previous["maxPriorityFeePerGas"] * bump),
            previous["maxPriorityFeePerGas"] + CONFIG.min_bump_wei,
        )

        max_fee = max(
            int(previous["maxFeePerGas"] * bump),
            previous["maxFeePerGas"] + CONFIG.min_bump_wei,
        )

        max_fee = max(max_fee, priority + base_fee)

        return {
            "type": 2,
            "maxPriorityFeePerGas": priority,
            "maxFeePerGas": max_fee,
        }

    priority = client.to_wei(
        CONFIG.max_priority_fee_gwei,
        "gwei",
    )

    max_fee = int(
        (base_fee * CONFIG.max_fee_multiplier) + priority
    )

    return {
        "type": 2,
        "maxPriorityFeePerGas": priority,
        "maxFeePerGas": max_fee,
    }


# =============================================================================
# TX BUILDING
# =============================================================================

def estimate_gas(tx: Dict[str, Any]) -> int:
    client = w3()

    try:
        gas = client.eth.estimate_gas(tx)

        gas = int(gas * CONFIG.gas_safety_multiplier)

        return max(gas, CONFIG.gas_limit_fallback)

    except Exception as e:
        logger.warning(f"Gas estimation failed: {e}")

        return CONFIG.gas_limit_fallback


def build_transaction(
    wallet: Dict[str, Any],
    nonce: int,
    fees: Dict[str, Any],
) -> Dict[str, Any]:
    client = w3()

    tx = {
        "chainId": CONFIG.chain_id,
        "nonce": nonce,
        "from": wallet["from"],
        "to": wallet["to"],
        "value": client.to_wei(wallet["value"], "ether"),
        **fees,
    }

    tx["gas"] = estimate_gas(tx)

    return tx


# =============================================================================
# RECEIPT WAITING
# =============================================================================

def wait_for_confirmations(
    tx_hash: bytes,
) -> Optional[Dict[str, Any]]:
    client = w3()

    start = time.time()

    while not STOP.is_set():
        elapsed = time.time() - start

        if elapsed > CONFIG.receipt_timeout:
            return None

        try:
            receipt = client.eth.get_transaction_receipt(tx_hash)

            if receipt:
                current_block = client.eth.block_number

                confirmations = (
                    current_block - receipt["blockNumber"]
                )

                if confirmations >= CONFIG.confirmations:
                    return receipt

        except TransactionNotFound:
            pass

        except Exception as e:
            logger.warning(f"Receipt poll error: {e}")

        time.sleep(CONFIG.receipt_poll)

    return None


# =============================================================================
# VALIDATION
# =============================================================================

def validate_wallet(wallet: Dict[str, Any]) -> Dict[str, Any]:
    client = w3()

    required = [
        "from_address",
        "to_address",
        "private_key",
        "value",
    ]

    for key in required:
        if key not in wallet:
            raise ValueError(f"Missing field: {key}")

    from_addr = client.to_checksum_address(
        wallet["from_address"]
    )

    to_addr = client.to_checksum_address(
        wallet["to_address"]
    )

    value = Decimal(str(wallet["value"]))

    if value <= 0:
        raise ValueError("Value must be positive")

    account = client.eth.account.from_key(
        wallet["private_key"]
    )

    if account.address.lower() != from_addr.lower():
        raise ValueError(
            f"Private key mismatch for {from_addr}"
        )

    return {
        "from": from_addr,
        "to": to_addr,
        "pk": wallet["private_key"],
        "value": value,
    }


# =============================================================================
# SEND LOGIC
# =============================================================================

def send(wallet: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    client = w3()

    address = wallet["from"]

    try:
        balance = client.eth.get_balance(address)

        if balance <= 0 and CONFIG.skip_zero_balance:
            logger.error(f"{short(address)} | zero balance")
            return None

        nonce = NONCE.get(address)

        fees = None
        last_tx_hash = None

        for attempt in range(CONFIG.max_send_retries):

            if STOP.is_set():
                return None

            try:
                fees = get_fees(fees)

                tx = build_transaction(
                    wallet=wallet,
                    nonce=nonce,
                    fees=fees,
                )

                estimated_total = (
                    tx["value"] +
                    (tx["gas"] * tx.get(
                        "maxFeePerGas",
                        tx.get("gasPrice", 0),
                    ))
                )

                if balance < estimated_total:
                    logger.error(
                        f"{short(address)} | "
                        f"insufficient funds "
                        f"(need={estimated_total}, "
                        f"have={balance})"
                    )
                    return None

                if CONFIG.dry_run:
                    logger.info(
                        f"[DRY RUN] "
                        f"{short(address)} -> "
                        f"{short(wallet['to'])} "
                        f"value={wallet['value']} ETH"
                    )

                    return {
                        "dry_run": True,
                        "tx": tx,
                    }

                signed = client.eth.account.sign_transaction(
                    tx,
                    wallet["pk"],
                )

                tx_hash = client.eth.send_raw_transaction(
                    signed.raw_transaction
                )

                tx_hash_hex = tx_hash.hex()

                last_tx_hash = tx_hash

                logger.info(
                    f"{short(address)} | "
                    f"sent | "
                    f"nonce={nonce} | "
                    f"hash={tx_hash_hex}"
                )

                receipt = wait_for_confirmations(tx_hash)

                if receipt:
                    logger.info(
                        f"{short(address)} | "
                        f"confirmed | "
                        f"block={receipt['blockNumber']}"
                    )

                    return dict(receipt)

                logger.warning(
                    f"{short(address)} | "
                    f"receipt timeout"
                )

            except Exception as e:
                msg = str(e).lower()

                if "nonce too low" in msg:
                    logger.warning(
                        f"{short(address)} | nonce too low"
                    )

                    nonce = NONCE.sync(address)
                    continue

                if (
                    "replacement transaction underpriced" in msg
                    or "underpriced" in msg
                ):
                    logger.warning(
                        f"{short(address)} | underpriced"
                    )
                    continue

                if "already known" in msg:
                    logger.warning(
                        f"{short(address)} | already known"
                    )

                    if last_tx_hash:
                        receipt = wait_for_confirmations(
                            last_tx_hash
                        )

                        if receipt:
                            return dict(receipt)

                    continue

                if "insufficient funds" in msg:
                    logger.error(
                        f"{short(address)} | "
                        f"insufficient funds"
                    )
                    return None

                logger.warning(
                    f"{short(address)} | "
                    f"attempt={attempt + 1} | "
                    f"error={e}"
                )

                time.sleep(backoff(attempt))

        logger.error(
            f"{short(address)} | exhausted retries"
        )

        return None

    except Exception as e:
        logger.exception(
            f"{short(address)} | fatal error: {e}"
        )
        return None


# =============================================================================
# IO
# =============================================================================

def load_wallets() -> List[Dict[str, Any]]:
    path = Path(CONFIG.wallets_file)

    if not path.exists():
        raise FileNotFoundError(
            f"Wallet file not found: {path}"
        )

    with path.open("r", encoding="utf-8") as f:
        raw = json.load(f)

    wallets = []

    for idx, item in enumerate(raw):
        try:
            wallets.append(validate_wallet(item))
        except Exception as e:
            logger.error(
                f"Invalid wallet at index={idx}: {e}"
            )

    return wallets


# =============================================================================
# RUNNER
# =============================================================================

def run(wallets: List[Dict[str, Any]]) -> None:
    total = len(wallets)

    success = 0
    failed = 0

    started = time.time()

    logger.info(
        f"Starting sender | "
        f"wallets={total} | "
        f"workers={CONFIG.max_workers}"
    )

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=CONFIG.max_workers,
        thread_name_prefix="sender",
    ) as executor:

        future_map = {
            executor.submit(send, wallet): wallet
            for wallet in wallets
        }

        completed = 0

        for future in concurrent.futures.as_completed(
            future_map
        ):
            if STOP.is_set():
                break

            wallet = future_map[future]

            completed += 1

            try:
                result = future.result()

                if result:
                    success += 1

                    atomic_append(
                        Path(CONFIG.success_file),
                        {
                            "from": wallet["from"],
                            "to": wallet["to"],
                            "value": str(wallet["value"]),
                            "receipt": result,
                        },
                        SUCCESS_LOCK,
                    )

                else:
                    failed += 1

                    atomic_append(
                        Path(CONFIG.failed_file),
                        wallet,
                        FAILED_LOCK,
                    )

            except Exception as e:
                failed += 1

                logger.exception(f"Worker crashed: {e}")

                atomic_append(
                    Path(CONFIG.failed_file),
                    wallet,
                    FAILED_LOCK,
                )

            elapsed = time.time() - started

            rate = completed / elapsed if elapsed > 0 else 0

            eta = (
                (total - completed) / rate
                if rate > 0 else 0
            )

            logger.info(
                f"Progress "
                f"{completed}/{total} | "
                f"success={success} | "
                f"failed={failed} | "
                f"rate={rate:.2f}/s | "
                f"eta={eta:.1f}s"
            )

    duration = time.time() - started

    logger.info(
        f"Finished | "
        f"success={success} | "
        f"failed={failed} | "
        f"duration={duration:.2f}s"
    )


# =============================================================================
# SHUTDOWN
# =============================================================================

def shutdown_handler(*_):
    logger.warning("Shutdown signal received")
    STOP.set()


# =============================================================================
# MAIN
# =============================================================================

def main():
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    try:
        wallets = load_wallets()

        if not wallets:
            logger.error("No valid wallets loaded")
            return

        run(wallets)

    except KeyboardInterrupt:
        logger.warning("Interrupted")

    except Exception as e:
        logger.exception(f"Fatal error: {e}")


if __name__ == "__main__":
    main()
