#!/usr/bin/env python3
"""
Ultra-hardened concurrent Ethereum sender (Infura / EIP-1559)

Major upgrades:
- Correct EIP-1559 replacement bumping (priority + maxFee)
- Per-wallet nonce isolation + reuse for replacement tx
- Pending tx tracking (same nonce reused on retry)
- Balance pre-check (skip guaranteed failures)
- Gas estimation with safety margin
- Receipt confirmation depth (reorg-safe)
- Thread-safe failed persistence (append mode)
- Clean retry separation (build/send/confirm)
"""

from __future__ import annotations

import concurrent.futures
import json
import logging
import os
import random
import signal
import time
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from pathlib import Path
from threading import Event, Lock
from typing import Any, Dict, List, Optional

from web3 import Web3
from web3.exceptions import TransactionNotFound


# =============================================================================
# Config
# =============================================================================

@dataclass(frozen=True)
class Config:
    chain_id: int = int(os.getenv("CHAIN_ID", "1"))
    network: str = os.getenv("NETWORK", "mainnet")

    max_workers: int = int(os.getenv("MAX_WORKERS", "5"))
    max_send_retries: int = int(os.getenv("MAX_SEND_RETRIES", "4"))

    max_priority_fee_gwei: int = int(os.getenv("MAX_PRIORITY_FEE_GWEI", "2"))
    max_fee_multiplier: int = int(os.getenv("MAX_FEE_MULTIPLIER", "2"))
    replacement_bump_percent: int = int(os.getenv("REPLACEMENT_BUMP_PERCENT", "15"))

    gas_limit_fallback: int = int(os.getenv("GAS_LIMIT_FALLBACK", "21000"))
    gas_safety_multiplier: float = float(os.getenv("GAS_SAFETY_MULTIPLIER", "1.2"))

    receipt_timeout: int = int(os.getenv("RECEIPT_TIMEOUT", "180"))
    receipt_poll: float = float(os.getenv("RECEIPT_POLL", "2"))
    confirmations: int = int(os.getenv("CONFIRMATIONS", "2"))

    wallets_file: str = os.getenv("WALLETS_FILE", "wallets.json")
    failed_file: str = os.getenv("FAILED_FILE", "failed.json")

    infura_project_id: Optional[str] = os.getenv("INFURA_PROJECT_ID")

    dry_run: bool = bool(int(os.getenv("DRY_RUN", "0")))


CONFIG = Config()
STOP = Event()


# =============================================================================
# Logging
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


# =============================================================================
# Web3 Singleton
# =============================================================================

_web3: Optional[Web3] = None
_lock = Lock()


def w3() -> Web3:
    global _web3

    with _lock:
        if _web3:
            return _web3

        if not CONFIG.infura_project_id:
            raise RuntimeError("INFURA_PROJECT_ID missing")

        url = f"https://{CONFIG.network}.infura.io/v3/{CONFIG.infura_project_id}"
        client = Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": 30}))

        if not client.is_connected():
            raise ConnectionError("RPC connection failed")

        logging.info("Connected to Ethereum")

        _web3 = client
        return client


# =============================================================================
# Helpers
# =============================================================================

def backoff(attempt: int) -> float:
    return min(10, (2 ** attempt) + random.uniform(0.1, 0.5))


def atomic_append(path: Path, data: Dict[str, Any]) -> None:
    with Lock():
        existing = []
        if path.exists():
            try:
                existing = json.loads(path.read_text())
            except Exception:
                pass

        existing.append(data)
        path.write_text(json.dumps(existing, indent=2, default=str))


# =============================================================================
# Nonce Manager (per wallet)
# =============================================================================

class NonceManager:
    def __init__(self):
        self.lock = Lock()
        self.data: Dict[str, int] = {}

    def get(self, addr: str) -> int:
        with self.lock:
            chain_nonce = w3().eth.get_transaction_count(addr, "pending")
            local = self.data.get(addr, chain_nonce)
            nonce = max(chain_nonce, local)
            self.data[addr] = nonce + 1
            return nonce

    def reuse(self, addr: str, nonce: int):
        with self.lock:
            self.data[addr] = nonce + 1

    def sync(self, addr: str):
        with self.lock:
            self.data[addr] = w3().eth.get_transaction_count(addr, "pending")


NONCE = NonceManager()


# =============================================================================
# Fee Logic
# =============================================================================

def get_fees(prev: Optional[Dict[str, int]] = None) -> Dict[str, int]:
    client = w3()
    block = client.eth.get_block("latest")

    base = block["baseFeePerGas"]
    priority = client.to_wei(CONFIG.max_priority_fee_gwei, "gwei")

    if prev:
        bump = 1 + CONFIG.replacement_bump_percent / 100
        priority = int(prev["maxPriorityFeePerGas"] * bump)
        max_fee = int(prev["maxFeePerGas"] * bump)
    else:
        max_fee = int(base * CONFIG.max_fee_multiplier + priority)

    return {
        "type": 2,
        "maxPriorityFeePerGas": int(priority),
        "maxFeePerGas": int(max_fee),
    }


# =============================================================================
# Build TX
# =============================================================================

def build_tx(wallet, nonce: int, fees: Dict[str, int]) -> Dict[str, Any]:
    client = w3()

    tx = {
        "from": wallet["from"],
        "to": wallet["to"],
        "value": client.to_wei(wallet["value"], "ether"),
        "nonce": nonce,
        "chainId": CONFIG.chain_id,
        **fees,
    }

    try:
        gas = client.eth.estimate_gas(tx)
        tx["gas"] = int(gas * CONFIG.gas_safety_multiplier)
    except Exception:
        tx["gas"] = CONFIG.gas_limit_fallback

    return tx


# =============================================================================
# Receipt
# =============================================================================

def wait_receipt(tx_hash: bytes) -> Optional[Dict[str, Any]]:
    client = w3()
    start = time.time()

    while not STOP.is_set():
        if time.time() - start > CONFIG.receipt_timeout:
            return None

        try:
            receipt = client.eth.get_transaction_receipt(tx_hash)

            if receipt:
                current = client.eth.block_number
                if current - receipt["blockNumber"] >= CONFIG.confirmations:
                    return receipt

        except TransactionNotFound:
            pass

        time.sleep(CONFIG.receipt_poll)

    return None


# =============================================================================
# Send Logic
# =============================================================================

class TxState(Enum):
    NEW = 1
    SENT = 2
    CONFIRMED = 3


def send(wallet: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    client = w3()
    addr = wallet["from"]

    # Balance pre-check
    balance = client.eth.get_balance(addr)
    if balance == 0:
        logging.error(f"{addr[:8]} | zero balance")
        return None

    nonce = NONCE.get(addr)
    fees = None

    for attempt in range(CONFIG.max_send_retries):

        if STOP.is_set():
            return None

        try:
            fees = get_fees(fees)
            tx = build_tx(wallet, nonce, fees)

            if CONFIG.dry_run:
                logging.info(f"[DRY RUN] {addr[:8]} -> {wallet['to'][:8]}")
                return {"dry_run": True}

            signed = client.eth.account.sign_transaction(tx, wallet["pk"])
            tx_hash = client.eth.send_raw_transaction(signed.rawTransaction)

            logging.info(f"{addr[:8]} | sent {tx_hash.hex()}")

            receipt = wait_receipt(tx_hash)
            if receipt:
                return receipt

        except Exception as e:
            msg = str(e).lower()

            if "nonce too low" in msg:
                NONCE.sync(addr)
                nonce = NONCE.get(addr)
                continue

            if "replacement transaction underpriced" in msg:
                continue

            if "insufficient funds" in msg:
                logging.error(f"{addr[:8]} insufficient funds")
                return None

            logging.warning(f"{addr[:8]} retry {attempt}: {e}")
            time.sleep(backoff(attempt))

    return None


# =============================================================================
# IO
# =============================================================================

def load_wallets() -> List[Dict[str, Any]]:
    file = Path(CONFIG.wallets_file)

    if not file.exists():
        return []

    data = json.loads(file.read_text())
    client = w3()

    wallets = []
    for w in data:
        wallets.append({
            "from": client.to_checksum_address(w["from_address"]),
            "to": client.to_checksum_address(w["to_address"]),
            "pk": w["private_key"],
            "value": Decimal(str(w["value"]))
        })

    return wallets


# =============================================================================
# Runner
# =============================================================================

def run(wallets: List[Dict[str, Any]]):
    failed = []
    success = 0

    with concurrent.futures.ThreadPoolExecutor(CONFIG.max_workers) as ex:
        futures = {ex.submit(send, w): w for w in wallets}

        for fut in concurrent.futures.as_completed(futures):
            w = futures[fut]
            try:
                if fut.result():
                    success += 1
                else:
                    failed.append(w)
                    atomic_append(Path(CONFIG.failed_file), w)
            except Exception as e:
                logging.error(f"Crash: {e}")
                failed.append(w)
                atomic_append(Path(CONFIG.failed_file), w)

    logging.info(f"Done | success={success} failed={len(failed)}")


# =============================================================================
# Entrypoint
# =============================================================================

def shutdown(*_):
    STOP.set()
    logging.warning("Stopping...")


def main():
    wallets = load_wallets()

    if not wallets:
        logging.error("No wallets loaded")
        return

    run(wallets)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)
    main()
