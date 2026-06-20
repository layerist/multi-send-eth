#!/usr/bin/env python3
from __future__ import annotations

import argparse
import concurrent.futures
import json
import logging
import os
import random
import signal
import threading
import time
from dataclasses import asdict, dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from web3 import Web3
from web3.exceptions import TransactionNotFound
from web3.providers.rpc import HTTPProvider


# =============================================================================
# CONFIG
# =============================================================================

@dataclass(frozen=True)
class Config:
    # Network
    chain_id: int = int(os.getenv("CHAIN_ID", "1"))
    network: str = os.getenv("NETWORK", "mainnet")
    rpc_url: Optional[str] = os.getenv("RPC_URL")
    infura_project_id: Optional[str] = os.getenv("INFURA_PROJECT_ID")

    # Concurrency
    max_workers: int = int(os.getenv("MAX_WORKERS", "20"))

    # Sending
    max_send_retries: int = int(os.getenv("MAX_SEND_RETRIES", "6"))
    replacement_bump_percent: int = int(os.getenv("REPLACEMENT_BUMP_PERCENT", "15"))
    min_bump_wei: int = int(os.getenv("MIN_BUMP_WEI", str(1_000_000_000)))  # 1 gwei

    # Fees
    max_priority_fee_gwei: float = float(os.getenv("MAX_PRIORITY_FEE_GWEI", "2"))
    max_fee_multiplier: float = float(os.getenv("MAX_FEE_MULTIPLIER", "2.0"))

    # Gas
    gas_limit_fallback: int = int(os.getenv("GAS_LIMIT_FALLBACK", "21000"))
    gas_safety_multiplier: float = float(os.getenv("GAS_SAFETY_MULTIPLIER", "1.15"))

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
    skip_zero_balance: bool = bool(int(os.getenv("SKIP_ZERO_BALANCE", "1")))


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
# MODELS
# =============================================================================

@dataclass(frozen=True)
class SendOutcome:
    ok: bool
    reason: str
    from_address: str
    to_address: str
    value: str
    tx_hash: Optional[str] = None
    nonce: Optional[int] = None
    receipt: Optional[Dict[str, Any]] = None
    dry_run_tx: Optional[Dict[str, Any]] = None


# =============================================================================
# SESSION / WEB3
# =============================================================================

def create_session() -> requests.Session:
    session = requests.Session()

    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset({"POST"}),
        respect_retry_after_header=True,
        raise_on_status=False,
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


def get_rpc_url() -> str:
    if CONFIG.rpc_url:
        return CONFIG.rpc_url

    if CONFIG.infura_project_id:
        return f"https://{CONFIG.network}.infura.io/v3/{CONFIG.infura_project_id}"

    raise RuntimeError("RPC_URL or INFURA_PROJECT_ID environment variable is required")


def w3() -> Web3:
    global _web3, _cached_chain_id

    if _web3 is not None:
        return _web3

    with NONCE_LOCK:
        if _web3 is not None:
            return _web3

        provider = HTTPProvider(
            get_rpc_url(),
            request_kwargs={
                "timeout": CONFIG.rpc_timeout,
                "session": HTTP_SESSION,
            },
        )
        client = Web3(provider)

        if not client.is_connected():
            raise ConnectionError("Unable to connect to Ethereum RPC")

        chain_id = int(client.eth.chain_id)
        if chain_id != CONFIG.chain_id:
            raise RuntimeError(
                f"Wrong chain connected (expected={CONFIG.chain_id}, got={chain_id})"
            )

        _cached_chain_id = chain_id
        _web3 = client
        logger.info("Connected to Ethereum (chain_id=%s, network=%s)", chain_id, CONFIG.network)
        return client


# =============================================================================
# HELPERS
# =============================================================================

def backoff(attempt: int) -> float:
    return min(30.0, 1.5 * (2 ** attempt)) + random.uniform(0.1, 1.0)


def interruptible_sleep(seconds: float) -> None:
    STOP.wait(max(0.0, seconds))


def jsonable(value: Any) -> Any:
    if isinstance(value, bytes):
        return "0x" + value.hex()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {str(k): jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [jsonable(v) for v in value]
    return value


def safe_json_load(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []

    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except json.JSONDecodeError as e:
        logger.warning("Cannot read JSON from %s: %s", path, e)
        return []
    except OSError as e:
        logger.warning("Cannot read %s: %s", path, e)
        return []


def atomic_write_json(path: Path, data: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp")

    with temp.open("w", encoding="utf-8") as f:
        json.dump(jsonable(data), f, indent=2, ensure_ascii=False)
        f.write("\n")

    os.replace(temp, path)


def atomic_append(path: Path, item: Dict[str, Any], lock: threading.Lock) -> None:
    with lock:
        existing = safe_json_load(path)
        existing.append(jsonable(item))
        atomic_write_json(path, existing)


def short(addr: str) -> str:
    return f"{addr[:6]}...{addr[-4:]}"


def safe_wallet_record(wallet: Dict[str, Any], reason: str, tx_hash: Optional[str] = None) -> Dict[str, Any]:
    record = {
        "from": wallet.get("from"),
        "to": wallet.get("to"),
        "value": str(wallet.get("value")),
        "reason": reason,
    }
    if tx_hash:
        record["tx_hash"] = tx_hash
    return record


def raw_transaction(signed_tx: Any) -> bytes:
    # web3.py v5 uses rawTransaction, v6/v7 uses raw_transaction.
    if hasattr(signed_tx, "raw_transaction"):
        return signed_tx.raw_transaction
    return signed_tx.rawTransaction


# =============================================================================
# NONCE MANAGER
# =============================================================================

class NonceManager:
    """Thread-safe nonce allocator. Prevents nonce collisions between workers."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.nonces: Dict[str, int] = {}

    def get(self, address: str) -> int:
        address = Web3.to_checksum_address(address)
        with self.lock:
            chain_nonce = int(w3().eth.get_transaction_count(address, "pending"))
            local_nonce = self.nonces.get(address)
            nonce = chain_nonce if local_nonce is None else max(chain_nonce, local_nonce)
            self.nonces[address] = nonce + 1
            return nonce

    def sync(self, address: str) -> int:
        address = Web3.to_checksum_address(address)
        with self.lock:
            nonce = int(w3().eth.get_transaction_count(address, "pending"))
            self.nonces[address] = nonce
            return nonce


NONCE = NonceManager()


# =============================================================================
# FEE LOGIC
# =============================================================================

def bump_legacy_fee(previous: Dict[str, int]) -> Dict[str, int]:
    bump = 1 + (CONFIG.replacement_bump_percent / 100)
    old = int(previous["gasPrice"])
    return {"gasPrice": max(int(old * bump), old + CONFIG.min_bump_wei)}


def get_fees(previous: Optional[Dict[str, int]] = None) -> Dict[str, int]:
    client = w3()
    latest_block = client.eth.get_block("latest")
    base_fee = latest_block.get("baseFeePerGas")

    if base_fee is None:
        if previous and "gasPrice" in previous:
            return bump_legacy_fee(previous)
        gas_price = int(client.eth.gas_price)
        return {"gasPrice": int(gas_price * CONFIG.max_fee_multiplier)}

    base_fee = int(base_fee)

    if previous and "maxPriorityFeePerGas" in previous and "maxFeePerGas" in previous:
        bump = 1 + (CONFIG.replacement_bump_percent / 100)
        old_priority = int(previous["maxPriorityFeePerGas"])
        old_max_fee = int(previous["maxFeePerGas"])

        priority = max(int(old_priority * bump), old_priority + CONFIG.min_bump_wei)
        max_fee = max(int(old_max_fee * bump), old_max_fee + CONFIG.min_bump_wei)
        max_fee = max(max_fee, priority + int(base_fee * CONFIG.max_fee_multiplier))

        return {
            "type": 2,
            "maxPriorityFeePerGas": priority,
            "maxFeePerGas": max_fee,
        }

    priority = int(client.to_wei(CONFIG.max_priority_fee_gwei, "gwei"))
    max_fee = int((base_fee * CONFIG.max_fee_multiplier) + priority)

    return {
        "type": 2,
        "maxPriorityFeePerGas": priority,
        "maxFeePerGas": max_fee,
    }


# =============================================================================
# TX BUILDING
# =============================================================================

def estimate_gas(tx: Dict[str, Any]) -> int:
    try:
        gas = int(w3().eth.estimate_gas(tx))
        gas = int(gas * CONFIG.gas_safety_multiplier)
        return max(gas, CONFIG.gas_limit_fallback)
    except Exception as e:
        logger.warning("Gas estimation failed, fallback=%s: %s", CONFIG.gas_limit_fallback, e)
        return CONFIG.gas_limit_fallback


def build_transaction(wallet: Dict[str, Any], nonce: int, fees: Dict[str, Any]) -> Dict[str, Any]:
    tx = {
        "chainId": CONFIG.chain_id,
        "nonce": nonce,
        "from": wallet["from"],
        "to": wallet["to"],
        "value": int(w3().to_wei(wallet["value"], "ether")),
        **fees,
    }
    tx["gas"] = estimate_gas(tx)
    return tx


def estimated_total_wei(tx: Dict[str, Any]) -> int:
    fee_per_gas = tx.get("maxFeePerGas", tx.get("gasPrice", 0))
    return int(tx["value"]) + (int(tx["gas"]) * int(fee_per_gas))


# =============================================================================
# RECEIPT WAITING
# =============================================================================

def wait_for_confirmations(tx_hash: bytes) -> Optional[Dict[str, Any]]:
    client = w3()
    start = time.time()

    while not STOP.is_set():
        if time.time() - start > CONFIG.receipt_timeout:
            return None

        try:
            receipt = client.eth.get_transaction_receipt(tx_hash)
            if receipt:
                if CONFIG.confirmations <= 1:
                    return jsonable(dict(receipt))

                current_block = int(client.eth.block_number)
                confirmations = (current_block - int(receipt["blockNumber"])) + 1
                if confirmations >= CONFIG.confirmations:
                    return jsonable(dict(receipt))

        except TransactionNotFound:
            pass
        except Exception as e:
            logger.warning("Receipt poll error: %s", e)

        interruptible_sleep(CONFIG.receipt_poll)

    return None


# =============================================================================
# VALIDATION
# =============================================================================

def validate_wallet(wallet: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(wallet, dict):
        raise ValueError("Wallet item must be an object")

    required = ["from_address", "to_address", "private_key", "value"]
    for key in required:
        if key not in wallet:
            raise ValueError(f"Missing field: {key}")

    client = w3()

    try:
        from_addr = client.to_checksum_address(str(wallet["from_address"]).strip())
        to_addr = client.to_checksum_address(str(wallet["to_address"]).strip())
    except Exception as e:
        raise ValueError(f"Invalid address: {e}") from e

    try:
        value = Decimal(str(wallet["value"]))
    except (InvalidOperation, ValueError) as e:
        raise ValueError(f"Invalid value: {wallet['value']}") from e

    if value <= 0:
        raise ValueError("Value must be positive")

    private_key = str(wallet["private_key"]).strip()
    account = client.eth.account.from_key(private_key)

    if account.address.lower() != from_addr.lower():
        raise ValueError(f"Private key mismatch for {from_addr}")

    return {
        "from": from_addr,
        "to": to_addr,
        "pk": private_key,
        "value": value,
    }


# =============================================================================
# SEND LOGIC
# =============================================================================

def send(wallet: Dict[str, Any]) -> SendOutcome:
    client = w3()
    address = wallet["from"]
    nonce: Optional[int] = None
    last_tx_hash: Optional[bytes] = None
    last_tx_hash_hex: Optional[str] = None

    try:
        if CONFIG.skip_zero_balance and int(client.eth.get_balance(address)) <= 0:
            logger.error("%s | zero balance", short(address))
            return SendOutcome(False, "zero_balance", address, wallet["to"], str(wallet["value"]))

        nonce = NONCE.get(address)
        fees: Optional[Dict[str, int]] = None

        for attempt in range(CONFIG.max_send_retries):
            if STOP.is_set():
                return SendOutcome(False, "stopped", address, wallet["to"], str(wallet["value"]), last_tx_hash_hex, nonce)

            try:
                fees = get_fees(fees)
                tx = build_transaction(wallet=wallet, nonce=nonce, fees=fees)

                balance = int(client.eth.get_balance(address))
                need = estimated_total_wei(tx)
                if balance < need:
                    reason = f"insufficient_funds need={need} have={balance}"
                    logger.error("%s | %s", short(address), reason)
                    return SendOutcome(False, reason, address, wallet["to"], str(wallet["value"]), last_tx_hash_hex, nonce)

                if CONFIG.dry_run:
                    logger.info(
                        "[DRY RUN] %s -> %s value=%s ETH nonce=%s",
                        short(address),
                        short(wallet["to"]),
                        wallet["value"],
                        nonce,
                    )
                    return SendOutcome(
                        True,
                        "dry_run",
                        address,
                        wallet["to"],
                        str(wallet["value"]),
                        nonce=nonce,
                        dry_run_tx=jsonable(tx),
                    )

                signed = client.eth.account.sign_transaction(tx, wallet["pk"])
                tx_hash = client.eth.send_raw_transaction(raw_transaction(signed))
                tx_hash_hex = tx_hash.hex()
                last_tx_hash = tx_hash
                last_tx_hash_hex = tx_hash_hex

                logger.info(
                    "%s | sent | nonce=%s | attempt=%s/%s | hash=%s",
                    short(address),
                    nonce,
                    attempt + 1,
                    CONFIG.max_send_retries,
                    tx_hash_hex,
                )

                receipt = wait_for_confirmations(tx_hash)
                if receipt:
                    if int(receipt.get("status", 0)) != 1:
                        logger.error("%s | transaction reverted | block=%s", short(address), receipt.get("blockNumber"))
                        return SendOutcome(False, "reverted", address, wallet["to"], str(wallet["value"]), tx_hash_hex, nonce, receipt)

                    logger.info("%s | confirmed | block=%s", short(address), receipt.get("blockNumber"))
                    return SendOutcome(True, "confirmed", address, wallet["to"], str(wallet["value"]), tx_hash_hex, nonce, receipt)

                logger.warning("%s | receipt timeout, trying replacement with bumped fee", short(address))

            except Exception as e:
                msg = str(e).lower()

                if "nonce too low" in msg or "already imported" in msg:
                    logger.warning("%s | nonce too low, syncing nonce", short(address))
                    nonce = NONCE.sync(address)
                    continue

                if "replacement transaction underpriced" in msg or "underpriced" in msg:
                    logger.warning("%s | underpriced, bumping fee", short(address))
                    interruptible_sleep(backoff(attempt))
                    continue

                if "already known" in msg:
                    logger.warning("%s | already known", short(address))
                    if last_tx_hash:
                        receipt = wait_for_confirmations(last_tx_hash)
                        if receipt:
                            return SendOutcome(True, "confirmed_after_already_known", address, wallet["to"], str(wallet["value"]), last_tx_hash_hex, nonce, receipt)
                    continue

                if "insufficient funds" in msg:
                    logger.error("%s | insufficient funds", short(address))
                    return SendOutcome(False, "insufficient_funds", address, wallet["to"], str(wallet["value"]), last_tx_hash_hex, nonce)

                logger.warning("%s | attempt=%s | error=%s", short(address), attempt + 1, e)
                interruptible_sleep(backoff(attempt))

        reason = "exhausted_retries"
        if last_tx_hash_hex:
            reason = "exhausted_retries_tx_may_still_confirm"
        logger.error("%s | %s", short(address), reason)
        return SendOutcome(False, reason, address, wallet["to"], str(wallet["value"]), last_tx_hash_hex, nonce)

    except Exception as e:
        logger.exception("%s | fatal error: %s", short(address), e)
        return SendOutcome(False, f"fatal_error: {e}", address, wallet.get("to", ""), str(wallet.get("value", "")), last_tx_hash_hex, nonce)


# =============================================================================
# IO
# =============================================================================

def load_wallets() -> List[Dict[str, Any]]:
    path = Path(CONFIG.wallets_file)
    if not path.exists():
        raise FileNotFoundError(f"Wallet file not found: {path}")

    with path.open("r", encoding="utf-8") as f:
        raw = json.load(f)

    if not isinstance(raw, list):
        raise ValueError("wallets.json must contain a list")

    wallets = []
    for idx, item in enumerate(raw):
        try:
            wallets.append(validate_wallet(item))
        except Exception as e:
            logger.error("Invalid wallet at index=%s: %s", idx, e)

    return wallets


def write_outcome(outcome: SendOutcome) -> None:
    path = Path(CONFIG.success_file if outcome.ok else CONFIG.failed_file)
    lock = SUCCESS_LOCK if outcome.ok else FAILED_LOCK
    atomic_append(path, asdict(outcome), lock)


# =============================================================================
# RUNNER
# =============================================================================

def run(wallets: List[Dict[str, Any]]) -> None:
    total = len(wallets)
    success = 0
    failed = 0
    completed = 0
    started = time.time()

    logger.info(
        "Starting sender | wallets=%s | workers=%s | dry_run=%s",
        total,
        CONFIG.max_workers,
        CONFIG.dry_run,
    )

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=CONFIG.max_workers,
        thread_name_prefix="sender",
    ) as executor:
        future_map = {executor.submit(send, wallet): wallet for wallet in wallets}

        try:
            for future in concurrent.futures.as_completed(future_map):
                completed += 1

                try:
                    outcome = future.result()
                except Exception as e:
                    wallet = future_map[future]
                    logger.exception("Worker crashed: %s", e)
                    outcome = SendOutcome(
                        False,
                        f"worker_crashed: {e}",
                        wallet.get("from", ""),
                        wallet.get("to", ""),
                        str(wallet.get("value", "")),
                    )

                if outcome.ok:
                    success += 1
                else:
                    failed += 1

                write_outcome(outcome)

                elapsed = time.time() - started
                rate = completed / elapsed if elapsed > 0 else 0
                eta = (total - completed) / rate if rate > 0 else 0

                logger.info(
                    "Progress %s/%s | success=%s | failed=%s | rate=%.2f/s | eta=%.1fs",
                    completed,
                    total,
                    success,
                    failed,
                    rate,
                    eta,
                )

                if STOP.is_set():
                    break

        finally:
            if STOP.is_set():
                for future in future_map:
                    future.cancel()

    duration = time.time() - started
    logger.info("Finished | success=%s | failed=%s | duration=%.2fs", success, failed, duration)


# =============================================================================
# CLI / SHUTDOWN
# =============================================================================

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ethereum batch transaction sender")
    parser.add_argument("--wallets", help="Path to wallets.json")
    parser.add_argument("--success", help="Path to success.json")
    parser.add_argument("--failed", help="Path to failed.json")
    parser.add_argument("--dry-run", action="store_true", help="Build transactions without broadcasting")
    return parser.parse_args()


def apply_args(args: argparse.Namespace) -> None:
    # Keep env-based Config immutable for most settings, but allow common file/runtime overrides.
    object.__setattr__(CONFIG, "wallets_file", args.wallets or CONFIG.wallets_file)
    object.__setattr__(CONFIG, "success_file", args.success or CONFIG.success_file)
    object.__setattr__(CONFIG, "failed_file", args.failed or CONFIG.failed_file)
    if args.dry_run:
        object.__setattr__(CONFIG, "dry_run", True)


def shutdown_handler(*_: Any) -> None:
    logger.warning("Shutdown signal received")
    STOP.set()


def install_signal_handlers() -> None:
    signal.signal(signal.SIGINT, shutdown_handler)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, shutdown_handler)


def main() -> None:
    install_signal_handlers()
    apply_args(parse_args())

    try:
        wallets = load_wallets()
        if not wallets:
            logger.error("No valid wallets loaded")
            return
        run(wallets)
    except KeyboardInterrupt:
        logger.warning("Interrupted")
    except Exception as e:
        logger.exception("Fatal error: %s", e)


if __name__ == "__main__":
    main()
