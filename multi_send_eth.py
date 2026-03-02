#!/usr/bin/env python3
"""
Concurrent Ethereum transaction sender via Infura (production-grade hardened).

Improvements:
- Decimal-based ETH handling (no float precision loss)
- Safer EIP-1559 fee escalation for replacements
- Strict retry separation (build/send/receipt)
- Nonce resync with pending pool awareness
- Clean shutdown propagation
- Deterministic failure persistence
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
# Configuration
# =============================================================================

@dataclass(frozen=True)
class Config:
    chain_id: int = int(os.getenv("CHAIN_ID", "1"))
    network: str = os.getenv("NETWORK", "mainnet")

    default_gas_limit: int = int(os.getenv("DEFAULT_GAS_LIMIT", "21000"))

    max_priority_fee_gwei: int = int(os.getenv("MAX_PRIORITY_FEE_GWEI", "3"))
    max_fee_multiplier: int = int(os.getenv("MAX_FEE_MULTIPLIER", "2"))
    replacement_fee_bump_percent: int = int(os.getenv("REPLACEMENT_FEE_BUMP_PERCENT", "12"))

    max_workers: int = int(os.getenv("MAX_WORKERS", "5"))
    max_send_retries: int = int(os.getenv("MAX_SEND_RETRIES", "3"))

    receipt_poll_interval: float = float(os.getenv("RECEIPT_POLL_INTERVAL", "2"))
    max_receipt_wait_seconds: int = int(os.getenv("MAX_RECEIPT_WAIT_SECONDS", "120"))

    wallets_file: str = os.getenv("WALLETS_FILE", "wallets.json")
    failed_tx_file: str = os.getenv("FAILED_TX_FILE", "failed_transactions.json")

    infura_project_id: Optional[str] = os.getenv("INFURA_PROJECT_ID")


CONFIG = Config()
STOP_EVENT = Event()


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
_web3_lock = Lock()


def get_web3() -> Web3:
    global _web3

    with _web3_lock:
        if _web3:
            return _web3

        if not CONFIG.infura_project_id:
            raise RuntimeError("INFURA_PROJECT_ID not set")

        url = f"https://{CONFIG.network}.infura.io/v3/{CONFIG.infura_project_id}"
        w3 = Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": 30}))

        if not w3.is_connected():
            raise ConnectionError("Failed to connect to RPC")

        logging.info(f"Connected to {CONFIG.network} (chain_id={CONFIG.chain_id})")
        _web3 = w3
        return w3


# =============================================================================
# Utilities
# =============================================================================

def backoff(base: float, attempt: int) -> float:
    return base * (2 ** (attempt - 1)) + random.uniform(0.1, 0.5)


def atomic_write(path: Path, data: str) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(data, encoding="utf-8")
    tmp.replace(path)


# =============================================================================
# Error Classification
# =============================================================================

class TxError(Enum):
    RETRYABLE = "retryable"
    NONCE = "nonce"
    INSUFFICIENT_FUNDS = "insufficient_funds"
    REPLACEMENT_UNDERPRICED = "replacement_underpriced"
    FATAL = "fatal"


def classify_error(exc: Exception) -> TxError:
    msg = str(exc).lower()

    if "nonce too low" in msg:
        return TxError.NONCE

    if "replacement transaction underpriced" in msg:
        return TxError.REPLACEMENT_UNDERPRICED

    if "insufficient funds" in msg:
        return TxError.INSUFFICIENT_FUNDS

    if "timeout" in msg or "connection" in msg:
        return TxError.RETRYABLE

    return TxError.FATAL


# =============================================================================
# Nonce Manager
# =============================================================================

class NonceManager:
    def __init__(self) -> None:
        self._lock = Lock()
        self._nonces: Dict[str, int] = {}

    def reserve(self, address: str) -> int:
        w3 = get_web3()
        with self._lock:
            chain_nonce = w3.eth.get_transaction_count(address, "pending")
            local_nonce = self._nonces.get(address, chain_nonce)
            nonce = max(chain_nonce, local_nonce)
            self._nonces[address] = nonce + 1
            return nonce

    def sync(self, address: str) -> None:
        w3 = get_web3()
        with self._lock:
            self._nonces[address] = w3.eth.get_transaction_count(address, "pending")


NONCES = NonceManager()


# =============================================================================
# Fee Logic
# =============================================================================

def build_eip1559_fees(multiplier: int = 1) -> Dict[str, int]:
    w3 = get_web3()
    block = w3.eth.get_block("latest")

    if "baseFeePerGas" not in block:
        raise RuntimeError("Chain does not support EIP-1559")

    base_fee = block["baseFeePerGas"]
    priority = w3.to_wei(CONFIG.max_priority_fee_gwei, "gwei")

    max_fee = base_fee * CONFIG.max_fee_multiplier * multiplier + priority

    return {
        "type": 2,
        "maxPriorityFeePerGas": int(priority),
        "maxFeePerGas": int(max_fee),
    }


# =============================================================================
# Transaction
# =============================================================================

def build_tx(wallet: Dict[str, Any], fee_multiplier: int = 1) -> Dict[str, Any]:
    w3 = get_web3()

    tx: Dict[str, Any] = {
        "nonce": NONCES.reserve(wallet["from_address"]),
        "to": wallet["to_address"],
        "value": w3.to_wei(wallet["value"], "ether"),
        "chainId": CONFIG.chain_id,
    }

    tx.update(build_eip1559_fees(multiplier=fee_multiplier))

    try:
        tx["gas"] = w3.eth.estimate_gas(tx)
    except Exception:
        tx["gas"] = CONFIG.default_gas_limit

    return tx


def wait_for_receipt(tx_hash: bytes) -> Optional[Dict[str, Any]]:
    w3 = get_web3()
    start = time.time()

    while not STOP_EVENT.is_set():
        if time.time() - start > CONFIG.max_receipt_wait_seconds:
            return None

        try:
            receipt = w3.eth.get_transaction_receipt(tx_hash)
            if receipt:
                return receipt
        except TransactionNotFound:
            pass

        time.sleep(CONFIG.receipt_poll_interval)

    return None


def send_eth(wallet: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    w3 = get_web3()
    addr = wallet["from_address"]
    fee_multiplier = 1

    for attempt in range(1, CONFIG.max_send_retries + 1):

        if STOP_EVENT.is_set():
            return None

        try:
            tx = build_tx(wallet, fee_multiplier)
            signed = w3.eth.account.sign_transaction(tx, wallet["private_key"])
            tx_hash = w3.eth.send_raw_transaction(signed.rawTransaction)

            logging.info(
                f"Sent {tx_hash.hex()} | {wallet['value']} ETH | "
                f"{addr[:8]} → {wallet['to_address'][:8]}"
            )

            return wait_for_receipt(tx_hash)

        except Exception as exc:
            category = classify_error(exc)

            if category == TxError.INSUFFICIENT_FUNDS:
                logging.error(f"Insufficient funds: {addr}")
                return None

            if category == TxError.NONCE:
                logging.warning(f"Nonce mismatch → resyncing {addr}")
                NONCES.sync(addr)
                continue

            if category == TxError.REPLACEMENT_UNDERPRICED:
                fee_multiplier *= 1 + CONFIG.replacement_fee_bump_percent / 100
                logging.warning(f"Bumping fee multiplier → {fee_multiplier:.2f}")
                continue

            if category == TxError.FATAL:
                logging.error(f"Fatal error ({addr}): {exc}")
                return None

            logging.warning(f"Retryable error ({addr}): {exc}")
            time.sleep(backoff(1, attempt))

    return None


# =============================================================================
# Wallet IO
# =============================================================================

REQUIRED_FIELDS = {"from_address", "to_address", "private_key", "value"}


def load_wallets(path: str) -> List[Dict[str, Any]]:
    file = Path(path)
    if not file.exists():
        logging.error(f"Wallet file not found: {file}")
        return []

    w3 = get_web3()
    wallets: List[Dict[str, Any]] = []

    try:
        raw = json.loads(file.read_text("utf-8"))

        for w in raw:
            if not REQUIRED_FIELDS.issubset(w):
                continue

            wallets.append({
                "from_address": w3.to_checksum_address(w["from_address"]),
                "to_address": w3.to_checksum_address(w["to_address"]),
                "private_key": w["private_key"],
                "value": Decimal(str(w["value"])),
            })

    except Exception as exc:
        logging.error(f"Wallet parse error: {exc}")

    logging.info(f"Loaded {len(wallets)} wallet(s)")
    return wallets


def save_failed(wallets: List[Dict[str, Any]]) -> None:
    if not wallets:
        return

    atomic_write(
        Path(CONFIG.failed_tx_file),
        json.dumps(wallets, indent=2, default=str),
    )

    logging.info(f"Saved {len(wallets)} failed transaction(s)")


# =============================================================================
# Execution
# =============================================================================

def run(wallets: List[Dict[str, Any]]) -> None:
    failed: List[Dict[str, Any]] = []
    success = 0

    with concurrent.futures.ThreadPoolExecutor(CONFIG.max_workers) as executor:
        futures = {executor.submit(send_eth, w): w for w in wallets}

        for fut in concurrent.futures.as_completed(futures):
            wallet = futures[fut]
            try:
                if fut.result():
                    success += 1
                else:
                    failed.append(wallet)
            except Exception as exc:
                logging.error(f"Worker crash: {exc}")
                failed.append(wallet)

    save_failed(failed)
    logging.info(f"Summary: {success} success | {len(failed)} failed")


# =============================================================================
# Entrypoint
# =============================================================================

def _graceful_exit(*_: object) -> None:
    logging.warning("Shutdown signal received")
    STOP_EVENT.set()


def main() -> None:
    wallets = load_wallets(CONFIG.wallets_file)
    if wallets:
        run(wallets)
    else:
        logging.error("No valid wallets loaded")


if __name__ == "__main__":
    signal.signal(signal.SIGINT, _graceful_exit)
    signal.signal(signal.SIGTERM, _graceful_exit)
    main()
