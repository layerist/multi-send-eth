#!/usr/bin/env python3
"""
Concurrent Ethereum transaction sender via Infura.

Features:
 - Multi-threaded transaction sending with retry logic
 - Exponential backoff for receipt polling
 - Robust wallet validation and error logging
 - Configurable via environment variables
"""

import concurrent.futures
import json
import logging
import os
import signal
import sys
import time
import random
from pathlib import Path
from typing import List, Dict, Any, Optional

from web3 import Web3
from web3.exceptions import TransactionNotFound

# --- Optional dotenv support ---
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


# --- Configuration ---
CONFIG: Dict[str, Any] = {
    "CHAIN_ID": int(os.getenv("CHAIN_ID", "1")),
    "DEFAULT_GAS_LIMIT": int(os.getenv("DEFAULT_GAS_LIMIT", "21000")),
    "DEFAULT_GAS_PRICE_GWEI": int(os.getenv("DEFAULT_GAS_PRICE_GWEI", "20")),
    "MAX_WORKERS": int(os.getenv("MAX_WORKERS", "5")),
    "RECEIPT_RETRY_DELAY": int(os.getenv("RECEIPT_RETRY_DELAY", "2")),
    "MAX_RECEIPT_RETRIES": int(os.getenv("MAX_RECEIPT_RETRIES", "6")),
    "WALLETS_FILE": os.getenv("WALLETS_FILE", "wallets.json"),
    "FAILED_TX_FILE": os.getenv("FAILED_TX_FILE", "failed_transactions.json"),
    "INFURA_PROJECT_ID": os.getenv("INFURA_PROJECT_ID"),
    "NETWORK": os.getenv("NETWORK", "mainnet"),
}


# --- Logging ---
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


# --- Web3 Setup ---
def init_web3() -> Web3:
    """Initialize and validate Infura connection."""
    project_id = CONFIG["INFURA_PROJECT_ID"]
    if not project_id:
        raise RuntimeError("Missing INFURA_PROJECT_ID in environment variables.")
    url = f"https://{CONFIG['NETWORK']}.infura.io/v3/{project_id}"
    w3 = Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": 20}))
    if not w3.is_connected():
        raise ConnectionError(f"Unable to connect to {CONFIG['NETWORK']} via Infura.")
    logging.info(f"🌐 Connected to {CONFIG['NETWORK']} (Chain ID: {CONFIG['CHAIN_ID']})")
    return w3


web3 = init_web3()


# --- Utility Helpers ---
def exponential_backoff(base_delay: float, attempt: int) -> float:
    """Compute exponential backoff with jitter."""
    return base_delay * (2 ** (attempt - 1)) + random.uniform(0.2, 1.2)


def wait_for_receipt(tx_hash: bytes) -> Optional[Dict[str, Any]]:
    """Wait for a transaction receipt with exponential backoff."""
    for attempt in range(1, CONFIG["MAX_RECEIPT_RETRIES"] + 1):
        try:
            receipt = web3.eth.get_transaction_receipt(tx_hash)
            if receipt:
                logging.info(f"✅ Confirmed: {tx_hash.hex()} | Block {receipt.blockNumber}")
                return receipt
        except TransactionNotFound:
            pass
        except Exception as ex:
            logging.warning(f"⚠️ Error fetching receipt {tx_hash.hex()}: {ex}")

        delay = exponential_backoff(CONFIG["RECEIPT_RETRY_DELAY"], attempt)
        logging.debug(f"⌛ Waiting ({attempt}/{CONFIG['MAX_RECEIPT_RETRIES']}) {delay:.1f}s for {tx_hash.hex()}")
        time.sleep(delay)

    logging.error(f"❌ No confirmation after {CONFIG['MAX_RECEIPT_RETRIES']} retries: {tx_hash.hex()}")
    return None


def send_eth(from_address: str, private_key: str, to_address: str, value: float) -> Optional[Dict[str, Any]]:
    """Send ETH from one wallet to another and wait for confirmation."""
    try:
        nonce = web3.eth.get_transaction_count(from_address, "pending")
        gas_price = web3.eth.gas_price or web3.to_wei(CONFIG["DEFAULT_GAS_PRICE_GWEI"], "gwei")

        tx = {
            "nonce": nonce,
            "to": to_address,
            "value": web3.to_wei(value, "ether"),
            "gas": CONFIG["DEFAULT_GAS_LIMIT"],
            "gasPrice": gas_price,
            "chainId": CONFIG["CHAIN_ID"],
        }

        try:
            tx["gas"] = web3.eth.estimate_gas(tx)
        except Exception:
            logging.debug("⚠️ Gas estimation failed. Using default gas limit.")

        signed_tx = web3.eth.account.sign_transaction(tx, private_key)
        tx_hash = web3.eth.send_raw_transaction(signed_tx.rawTransaction)

        logging.info(f"📤 Sent: {tx_hash.hex()} | {value} ETH | {from_address[:8]}... → {to_address[:8]}...")
        return wait_for_receipt(tx_hash)

    except ValueError as ve:
        msg = str(ve)
        if "insufficient funds" in msg:
            logging.error(f"❌ Insufficient funds for {from_address}")
        elif "replacement transaction underpriced" in msg:
            logging.warning(f"⚠️ Nonce conflict for {from_address}")
        else:
            logging.error(f"⚠️ Transaction rejected: {ve}")
    except Exception as ex:
        logging.exception(f"💥 Unexpected error from {from_address[:8]}...: {ex}")
    return None


# --- Wallet I/O ---
def load_wallets(file_path: str) -> List[Dict[str, Any]]:
    """Load and validate wallet entries."""
    path = Path(file_path)
    if not path.exists():
        logging.error(f"❌ Wallet file not found: {path}")
        return []

    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)

        if not isinstance(data, list):
            raise ValueError("Wallet JSON must contain a list of objects.")

        wallets: List[Dict[str, Any]] = []
        required = {"from_address", "private_key", "to_address", "value"}

        for entry in data:
            if isinstance(entry, dict) and required.issubset(entry):
                try:
                    entry["value"] = float(entry["value"])
                    wallets.append(entry)
                except ValueError:
                    logging.warning(f"⚠️ Invalid 'value' type: {entry}")
            else:
                logging.warning(f"⚠️ Skipped invalid entry: {entry}")

        logging.info(f"🔑 Loaded {len(wallets)} valid wallet(s).")
        return wallets

    except json.JSONDecodeError:
        logging.error("❌ Invalid JSON format.")
    except Exception as ex:
        logging.error(f"❌ Error reading wallet file: {ex}")
    return []


def save_failed_wallets(wallets: List[Dict[str, Any]]) -> None:
    """Save failed wallet transactions to file."""
    if not wallets:
        return
    try:
        with open(CONFIG["FAILED_TX_FILE"], "w", encoding="utf-8") as f:
            json.dump(wallets, f, indent=2)
        logging.info(f"💾 Saved {len(wallets)} failed transaction(s).")
    except Exception as ex:
        logging.error(f"❌ Failed to save failed transactions: {ex}")


# --- Worker Logic ---
def process_transaction(wallet: Dict[str, Any]) -> bool:
    """Process one transaction and return success status."""
    start = time.perf_counter()
    # Add a small random delay to reduce nonce collisions under load
    time.sleep(random.uniform(0.1, 0.6))

    receipt = send_eth(
        from_address=wallet["from_address"],
        private_key=wallet["private_key"],
        to_address=wallet["to_address"],
        value=wallet["value"],
    )

    elapsed = time.perf_counter() - start
    if receipt:
        logging.info(f"⏱ Completed {wallet['from_address'][:8]}... → {wallet['to_address'][:8]}... in {elapsed:.2f}s")
        return True
    logging.warning(f"🚫 Failed transaction: {wallet['from_address'][:8]}...")
    return False


def process_all_wallets(wallets: List[Dict[str, Any]]) -> None:
    """Run all wallet transactions concurrently."""
    if not wallets:
        logging.warning("⚠️ No wallets to process.")
        return

    logging.info(f"🚀 Starting {len(wallets)} transaction(s) using {CONFIG['MAX_WORKERS']} threads...")

    failed_wallets: List[Dict[str, Any]] = []
    success_count = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=CONFIG["MAX_WORKERS"]) as executor:
        futures = {executor.submit(process_transaction, w): w for w in wallets}

        try:
            for future in concurrent.futures.as_completed(futures):
                wallet = futures[future]
                try:
                    if future.result():
                        success_count += 1
                    else:
                        failed_wallets.append(wallet)
                except Exception as ex:
                    logging.error(f"❗ Error for {wallet['from_address'][:8]}...: {ex}")
                    failed_wallets.append(wallet)
        except KeyboardInterrupt:
            logging.warning("🛑 Interrupted by user, stopping all threads...")
            executor.shutdown(wait=False, cancel_futures=True)
            save_failed_wallets(failed_wallets)
            sys.exit(0)

    save_failed_wallets(failed_wallets)
    logging.info(f"📊 Summary: {success_count} succeeded, {len(failed_wallets)} failed.")


# --- Entrypoint ---
def main() -> None:
    try:
        wallets = load_wallets(CONFIG["WALLETS_FILE"])
        if wallets:
            process_all_wallets(wallets)
        else:
            logging.error("🚫 No valid wallets found. Exiting.")
    except Exception as ex:
        logging.critical(f"💀 Fatal error: {ex}")
        sys.exit(1)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, lambda *_: sys.exit(0))
    main()
