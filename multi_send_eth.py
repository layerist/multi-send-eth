#!/usr/bin/env python3
"""
Concurrent Ethereum transaction sender via Infura (production-grade).

Design goals:
- Deterministic nonce management across threads
- Explicit EIP-1559 fee control with safety caps
- Clear error classification (retryable vs fatal)
- Graceful shutdown without orphan workers
- Atomic persistence of failed transactions
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
from enum import Enum
from pathlib import Path
from threading import Event, Lock
from typing import Any, Dict, List, Optional

from web3 import Web3
from web3.exceptions import TransactionNotFound

# -----------------------------------------------------------------------------
# Optional dotenv
# -----------------------------------------------------------------------------
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


# =============================================================================
# Configuration
# =============================================================================

@dataclass(frozen=True)
class Config:
    chain_id: int = int(os.getenv("CHAIN_ID", "1"))
    network: str = os.getenv("NETWORK", "mainnet")

    default_gas_limit: int = int(os.getenv("DEFAULT_GAS_LIMIT", "21000"))
    fallback_gas_price_gwei: int = int(os.getenv("DEFAULT_GAS_PRICE_GWEI", "20"))

    max_priority_fee_gwei: int = int(os.getenv("MAX_PRIORITY_FEE_GWEI", "3"))
    max_fee_multiplier: int = int(os.getenv("MAX_FEE_MULTIPLIER", "2"))

    max_workers: int = int(os.getenv("MAX_WORKERS", "5"))
    max_send_retries: int = int(os.getenv("MAX_SEND_RETRIES", "3"))

    receipt_retry_delay: float = float(os.getenv("RECEIPT_RETRY_DELAY", "2"))
    max_receipt_retries: int = int(os.getenv("MAX_RECEIPT_RETRIES", "6"))

    wallets_file: str = os.getenv("WALLETS_FILE", "wallets.json")
    failed_tx_file: str = os.getenv("FAILED_TX_FILE", "failed_transactions.json")

    infura_project_id: Optional[str] = os.getenv("INFURA_PROJECT_ID")


CONFIG = Config()
STOP_EVENT = Event()


# =============================================================================
# Logging
# =============================================================================

def setup_logging() -> None:
    try:
        import colorlog
        handler = colorlog.StreamHandler()
        handler.setFormatter(
            colorlog.ColoredFormatter(
                "%(log_color)s%(asctime)s [%(levelname)s]%(reset)s %(message)s",
                log_colors={
                    "DEBUG": "cyan",
                    "INFO": "green",
                    "WARNING": "yellow",
                    "ERROR": "red",
                    "CRITICAL": "bold_red",
                },
            )
        )
        logging.basicConfig(level=logging.INFO, handlers=[handler])
    except ImportError:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(message)s",
        )


setup_logging()


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
            raise RuntimeError("INFURA_PROJECT_ID is not set")

        url = f"https://{CONFIG.network}.infura.io/v3/{CONFIG.infura_project_id}"
        w3 = Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": 20}))

        if not w3.is_connected():
            raise ConnectionError("Failed to connect to Infura")

        logging.info(f"Connected to {CONFIG.network} (chain_id={CONFIG.chain_id})")
        _web3 = w3
        return w3


# =============================================================================
# Utilities
# =============================================================================

def backoff(base: float, attempt: int) -> float:
    return base * (2 ** (attempt - 1)) + random.uniform(0.1, 0.6)


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
    FATAL = "fatal"


def classify_error(exc: Exception) -> TxError:
    msg = str(exc).lower()
    if "nonce too low" in msg or "replacement transaction underpriced" in msg:
        return TxError.NONCE
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
            if address not in self._nonces:
                self._nonces[address] = w3.eth.get_transaction_count(address, "pending")
            nonce = self._nonces[address]
            self._nonces[address] += 1
            return nonce

    def reset(self, address: str) -> None:
        w3 = get_web3()
        with self._lock:
            self._nonces[address] = w3.eth.get_transaction_count(address, "pending")


NONCES = NonceManager()


# =============================================================================
# Receipt Handling
# =============================================================================

def wait_for_receipt(tx_hash: bytes) -> Optional[Dict[str, Any]]:
    w3 = get_web3()
    hex_hash = tx_hash.hex()

    for attempt in range(1, CONFIG.max_receipt_retries + 1):
        if STOP_EVENT.is_set():
            return None
        try:
            receipt = w3.eth.get_transaction_receipt(tx_hash)
            if receipt:
                logging.info(f"Confirmed {hex_hash} in block {receipt.blockNumber}")
                return receipt
        except TransactionNotFound:
            pass
        except Exception as exc:
            logging.warning(f"Receipt error {hex_hash}: {exc}")

        time.sleep(backoff(CONFIG.receipt_retry_delay, attempt))

    logging.error(f"Receipt timeout: {hex_hash}")
    return None


# =============================================================================
# Transaction Building
# =============================================================================

def build_tx(wallet: Dict[str, Any]) -> Dict[str, Any]:
    w3 = get_web3()

    tx: Dict[str, Any] = {
        "nonce": NONCES.reserve(wallet["from_address"]),
        "to": wallet["to_address"],
        "value": w3.to_wei(wallet["value"], "ether"),
        "chainId": CONFIG.chain_id,
    }

    # EIP-1559 preferred
    try:
        block = w3.eth.get_block("latest")
        base_fee = block["baseFeePerGas"]
        priority = w3.to_wei(CONFIG.max_priority_fee_gwei, "gwei")

        max_fee = base_fee * CONFIG.max_fee_multiplier + priority
        tx.update({
            "type": 2,
            "maxPriorityFeePerGas": priority,
            "maxFeePerGas": max_fee,
        })
    except Exception:
        tx["gasPrice"] = w3.to_wei(CONFIG.fallback_gas_price_gwei, "gwei")

    try:
        tx["gas"] = w3.eth.estimate_gas(tx)
    except Exception:
        tx["gas"] = CONFIG.default_gas_limit

    return tx


# =============================================================================
# Transaction Sending
# =============================================================================

def send_eth(wallet: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    w3 = get_web3()
    addr = wallet["from_address"]

    for attempt in range(1, CONFIG.max_send_retries + 1):
        if STOP_EVENT.is_set():
            return None
        try:
            tx = build_tx(wallet)
            signed = w3.eth.account.sign_transaction(tx, wallet["private_key"])
            tx_hash = w3.eth.send_raw_transaction(signed.rawTransaction)

            logging.info(
                f"Sent {tx_hash.hex()} | {wallet['value']} ETH | "
                f"{addr[:8]} → {wallet['to_address'][:8]}"
            )

            return wait_for_receipt(tx_hash)

        except Exception as exc:
            category = classify_error(exc)

            if category is TxError.INSUFFICIENT_FUNDS:
                logging.error(f"Insufficient funds: {addr}")
                return None

            if category is TxError.NONCE:
                logging.warning(f"Nonce conflict, resetting nonce ({addr})")
                NONCES.reset(addr)

            if category is TxError.FATAL:
                logging.error(f"Fatal tx error ({addr}): {exc}")
                return None

            logging.warning(
                f"Retryable error ({addr}, attempt {attempt}): {exc}"
            )
            time.sleep(backoff(1.0, attempt))

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

    wallets: List[Dict[str, Any]] = []
    w3 = get_web3()

    try:
        raw = json.loads(file.read_text("utf-8"))
        if not isinstance(raw, list):
            raise ValueError("Wallet file must contain a list")

        for w in raw:
            if not isinstance(w, dict) or not REQUIRED_FIELDS.issubset(w):
                continue
            try:
                wallets.append({
                    "from_address": w3.to_checksum_address(w["from_address"]),
                    "to_address": w3.to_checksum_address(w["to_address"]),
                    "private_key": w["private_key"],
                    "value": float(w["value"]),
                })
            except Exception:
                logging.warning(f"Invalid wallet skipped: {w}")

    except Exception as exc:
        logging.error(f"Wallet parse error: {exc}")

    logging.info(f"Loaded {len(wallets)} wallet(s)")
    return wallets


def save_failed(wallets: List[Dict[str, Any]]) -> None:
    if not wallets:
        return
    try:
        atomic_write(
            Path(CONFIG.failed_tx_file),
            json.dumps(wallets, indent=2),
        )
        logging.info(f"Saved {len(wallets)} failed transaction(s)")
    except Exception as exc:
        logging.error(f"Failed to persist failed txs: {exc}")


# =============================================================================
# Execution
# =============================================================================

def process_wallet(wallet: Dict[str, Any]) -> bool:
    start = time.perf_counter()
    receipt = send_eth(wallet)
    elapsed = time.perf_counter() - start

    if receipt:
        logging.info(f"Completed in {elapsed:.2f}s | {wallet['from_address'][:8]}")
        return True

    logging.warning(f"Failed: {wallet['from_address'][:8]}")
    return False


def run(wallets: List[Dict[str, Any]]) -> None:
    failed: List[Dict[str, Any]] = []
    success = 0

    with concurrent.futures.ThreadPoolExecutor(CONFIG.max_workers) as executor:
        futures = {executor.submit(process_wallet, w): w for w in wallets}

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
    logging.info(f"Summary: {success} success, {len(failed)} failed")


# =============================================================================
# Entrypoint
# =============================================================================

def main() -> None:
    wallets = load_wallets(CONFIG.wallets_file)
    if not wallets:
        logging.error("No valid wallets loaded")
        return
    run(wallets)


def _graceful_exit(*_: object) -> None:
    logging.warning("Shutdown signal received")
    STOP_EVENT.set()


if __name__ == "__main__":
    signal.signal(signal.SIGINT, _graceful_exit)
    signal.signal(signal.SIGTERM, _graceful_exit)
    main()
