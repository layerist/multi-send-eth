#!/usr/bin/env python3
"""
Concurrent Ethereum transaction sender via Infura.

Improvements:
 - Cleaner architecture and reduced global state
 - Thread-safe Web3 session initialization
 - Better nonce handling under concurrency
 - Centralized retry logic & error classification
 - More defensive JSON parsing and value validation
 - Full type annotations
 - Consistent logging & emoji cleanup
 - Stronger receipt + send retry logic
"""

from __future__ import annotations

import concurrent.futures
import json
import logging
import os
import random
import signal
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Any, Dict, List, Optional

from web3 import Web3
from web3.exceptions import TransactionNotFound

# --- Optional dotenv support ---
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
    default_gas_limit: int = int(os.getenv("DEFAULT_GAS_LIMIT", "21000"))
    default_gas_price_gwei: int = int(os.getenv("DEFAULT_GAS_PRICE_GWEI", "20"))
    max_workers: int = int(os.getenv("MAX_WORKERS", "5"))
    receipt_retry_delay: float = float(os.getenv("RECEIPT_RETRY_DELAY", "2"))
    max_receipt_retries: int = int(os.getenv("MAX_RECEIPT_RETRIES", "6"))
    wallets_file: str = os.getenv("WALLETS_FILE", "wallets.json")
    failed_tx_file: str = os.getenv("FAILED_TX_FILE", "failed_transactions.json")
    infura_project_id: Optional[str] = os.getenv("INFURA_PROJECT_ID")
    network: str = os.getenv("NETWORK", "mainnet")


CONFIG = Config()

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
            handlers=[logging.StreamHandler()],
        )

setup_logging()


# =============================================================================
# Web3 Initialization (thread-safe)
# =============================================================================

_web3_instance: Optional[Web3] = None
_web3_lock = Lock()


def get_web3() -> Web3:
    global _web3_instance
    with _web3_lock:
        if _web3_instance is not None:
            return _web3_instance

        if not CONFIG.infura_project_id:
            raise RuntimeError("Missing INFURA_PROJECT_ID in environment variables.")

        url = f"https://{CONFIG.network}.infura.io/v3/{CONFIG.infura_project_id}"
        w3 = Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": 20}))

        if not w3.is_connected():
            raise ConnectionError(f"Unable to connect to {CONFIG.network} via Infura.")

        logging.info(f"Connected to {CONFIG.network} (chain_id={CONFIG.chain_id})")
        _web3_instance = w3
        return w3


# =============================================================================
# Utility Functions
# =============================================================================

def exponential_backoff(base: float, attempt: int) -> float:
    """Exponential delay with jitter."""
    return base * (2 ** (attempt - 1)) + random.uniform(0.2, 1.0)


def wait_for_receipt(tx_hash: bytes) -> Optional[Dict[str, Any]]:
    w3 = get_web3()
    hex_hash = tx_hash.hex()

    for attempt in range(1, CONFIG.max_receipt_retries + 1):
        try:
            receipt = w3.eth.get_transaction_receipt(tx_hash)
            if receipt:
                logging.info(f"Confirmed {hex_hash} at block {receipt.blockNumber}")
                return receipt
        except TransactionNotFound:
            pass
        except Exception as ex:
            logging.warning(f"Error reading receipt for {hex_hash}: {ex}")

        delay = exponential_backoff(CONFIG.receipt_retry_delay, attempt)
        logging.debug(f"Waiting ({attempt}/{CONFIG.max_receipt_retries}) {delay:.1f}s: {hex_hash}")
        time.sleep(delay)

    logging.error(f"No confirmation after retries: {hex_hash}")
    return None


# =============================================================================
# Sending ETH
# =============================================================================

_nonce_lock = Lock()
_nonces: Dict[str, int] = {}


def get_thread_safe_nonce(address: str) -> int:
    """Avoid nonce collisions under heavy concurrency."""
    w3 = get_web3()
    with _nonce_lock:
        if address not in _nonces:
            _nonces[address] = w3.eth.get_transaction_count(address, "pending")
        else:
            _nonces[address] += 1
        return _nonces[address]


def send_eth(wallet: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    w3 = get_web3()

    from_addr = wallet["from_address"]
    to_addr = wallet["to_address"]
    value_eth = wallet["value"]
    pk = wallet["private_key"]

    try:
        nonce = get_thread_safe_nonce(from_addr)
        gas_price = w3.eth.gas_price or w3.to_wei(CONFIG.default_gas_price_gwei, "gwei")

        tx = {
            "nonce": nonce,
            "to": to_addr,
            "value": w3.to_wei(value_eth, "ether"),
            "gas": CONFIG.default_gas_limit,
            "gasPrice": gas_price,
            "chainId": CONFIG.chain_id,
        }

        # Try estimating gas
        try:
            tx["gas"] = w3.eth.estimate_gas(tx)
        except Exception:
            logging.debug(f"Gas estimation failed for {from_addr}. Using default gas limit.")

        signed = w3.eth.account.sign_transaction(tx, pk)
        tx_hash = w3.eth.send_raw_transaction(signed.rawTransaction)

        logging.info(f"Sent {tx_hash.hex()} | {value_eth} ETH | {from_addr[:8]} → {to_addr[:8]}")
        return wait_for_receipt(tx_hash)

    except ValueError as ve:
        msg = str(ve)
        if "insufficient funds" in msg:
            logging.error(f"Insufficient funds: {from_addr}")
        elif "underpriced" in msg or "nonce too low" in msg:
            logging.warning(f"Nonce issue for {from_addr}")
        else:
            logging.error(f"Transaction rejected: {msg}")
    except Exception as ex:
        logging.exception(f"Unhandled error from {from_addr[:8]}: {ex}")

    return None


# =============================================================================
# Wallet Loading / Saving
# =============================================================================

def load_wallets(path: str) -> List[Dict[str, Any]]:
    file = Path(path)
    if not file.exists():
        logging.error(f"Wallet file not found: {file}")
        return []

    try:
        data = json.loads(file.read_text(encoding="utf-8"))
        if not isinstance(data, list):
            raise ValueError("Wallet file must contain a list.")

        wallets: List[Dict[str, Any]] = []
        required = {"from_address", "private_key", "to_address", "value"}

        for entry in data:
            if not isinstance(entry, dict) or not required.issubset(entry):
                logging.warning(f"Skipped malformed wallet: {entry}")
                continue
            try:
                entry["value"] = float(entry["value"])
            except Exception:
                logging.warning(f"Invalid ETH value in wallet: {entry}")
                continue

            wallets.append(entry)

        logging.info(f"Loaded {len(wallets)} wallet(s)")
        return wallets

    except Exception as ex:
        logging.error(f"Error reading wallet file: {ex}")
        return []


def save_failed(wallets: List[Dict[str, Any]]) -> None:
    if not wallets:
        return
    try:
        Path(CONFIG.failed_tx_file).write_text(json.dumps(wallets, indent=2), encoding="utf-8")
        logging.info(f"Saved {len(wallets)} failed transaction(s)")
    except Exception as ex:
        logging.error(f"Error writing failed transactions: {ex}")


# =============================================================================
# Processing Logic
# =============================================================================

def process_wallet(wallet: Dict[str, Any]) -> bool:
    start = time.perf_counter()
    time.sleep(random.uniform(0.1, 0.5))  # reduce nonce collisions

    receipt = send_eth(wallet)
    elapsed = time.perf_counter() - start

    if receipt:
        logging.info(f"Completed in {elapsed:.2f}s | {wallet['from_address'][:8]}")
        return True

    logging.warning(f"Failed: {wallet['from_address'][:8]}")
    return False


def run_concurrent(wallets: List[Dict[str, Any]]) -> None:
    if not wallets:
        logging.warning("No wallets to process.")
        return

    logging.info(f"Starting {len(wallets)} transactions using {CONFIG.max_workers} threads")

    failed: List[Dict[str, Any]] = []
    success = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=CONFIG.max_workers) as executor:
        futures = {executor.submit(process_wallet, w): w for w in wallets}

        try:
            for fut in concurrent.futures.as_completed(futures):
                wallet = futures[fut]
                try:
                    if fut.result():
                        success += 1
                    else:
                        failed.append(wallet)
                except Exception as ex:
                    logging.error(f"Error during processing wallet: {ex}")
                    failed.append(wallet)

        except KeyboardInterrupt:
            logging.warning("Interrupted by user — terminating threads.")
            executor.shutdown(cancel_futures=True)
            save_failed(failed)
            sys.exit(1)

    save_failed(failed)
    logging.info(f"Summary: {success} succeeded, {len(failed)} failed")


# =============================================================================
# Entrypoint
# =============================================================================

def main() -> None:
    try:
        wallets = load_wallets(CONFIG.wallets_file)
        if not wallets:
            logging.error("No valid wallets found. Exiting.")
            return
        run_concurrent(wallets)
    except Exception as ex:
        logging.critical(f"Fatal error: {ex}")
        sys.exit(1)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, lambda *_: sys.exit(0))
    main()
