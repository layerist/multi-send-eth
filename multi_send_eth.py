import concurrent.futures
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import List, Dict, Optional, Any

from web3 import Web3
from web3.exceptions import TransactionNotFound


# --- Configuration ---
DEFAULT_GAS_LIMIT: int = int(os.getenv("DEFAULT_GAS_LIMIT", "21000"))
DEFAULT_GAS_PRICE_GWEI: int = int(os.getenv("DEFAULT_GAS_PRICE_GWEI", "20"))
MAX_WORKERS: int = int(os.getenv("MAX_WORKERS", "5"))
RECEIPT_RETRY_DELAY: int = int(os.getenv("RECEIPT_RETRY_DELAY", "2"))
MAX_RECEIPT_RETRIES: int = int(os.getenv("MAX_RECEIPT_RETRIES", "5"))
CHAIN_ID: int = int(os.getenv("CHAIN_ID", "1"))  # Mainnet default
WALLETS_FILE: str = os.getenv("WALLETS_FILE", "wallets.json")
FAILED_TX_FILE: str = os.getenv("FAILED_TX_FILE", "failed_transactions.json")


# --- Logging Setup ---
try:
    import colorlog

    handler = colorlog.StreamHandler()
    handler.setFormatter(colorlog.ColoredFormatter(
        "%(log_color)s%(asctime)s [%(levelname)s]%(reset)s %(message)s",
        log_colors={
            "DEBUG": "cyan",
            "INFO": "green",
            "WARNING": "yellow",
            "ERROR": "red",
            "CRITICAL": "bold_red",
        },
    ))
    logging.basicConfig(level=logging.INFO, handlers=[handler])
except ImportError:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler()],
    )


# --- Environment & Web3 Setup ---
def get_env_var(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: '{name}'")
    return value


def init_web3() -> Web3:
    project_id = get_env_var("INFURA_PROJECT_ID")
    url = f"https://mainnet.infura.io/v3/{project_id}"
    w3 = Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": 10}))
    if not w3.is_connected():
        raise ConnectionError("Unable to connect to Ethereum via Infura.")
    return w3


web3: Web3 = init_web3()


# --- Transaction Helpers ---
def wait_for_receipt(tx_hash: bytes) -> Optional[Dict[str, Any]]:
    """Poll Infura for a transaction receipt with exponential backoff."""
    delay = RECEIPT_RETRY_DELAY
    for attempt in range(1, MAX_RECEIPT_RETRIES + 1):
        try:
            receipt = web3.eth.get_transaction_receipt(tx_hash)
            logging.info(f"✅ Confirmed: {tx_hash.hex()} | Block: {receipt.blockNumber}")
            return receipt
        except TransactionNotFound:
            logging.debug(f"⌛ Waiting [{attempt}/{MAX_RECEIPT_RETRIES}] for {tx_hash.hex()}")
            time.sleep(delay)
            delay *= 2
    logging.error(f"❌ No confirmation after {MAX_RECEIPT_RETRIES} retries: {tx_hash.hex()}")
    return None


def send_eth(
    from_address: str,
    private_key: str,
    to_address: str,
    value: float,
) -> Optional[Dict[str, Any]]:
    """Send ETH from one address to another and wait for confirmation."""
    try:
        nonce = web3.eth.get_transaction_count(from_address, "pending")
        gas_price = web3.eth.gas_price or web3.to_wei(DEFAULT_GAS_PRICE_GWEI, "gwei")

        tx: Dict[str, Any] = {
            "nonce": nonce,
            "to": to_address,
            "value": web3.to_wei(value, "ether"),
            "gas": DEFAULT_GAS_LIMIT,
            "gasPrice": gas_price,
            "chainId": CHAIN_ID,
        }

        try:
            tx["gas"] = web3.eth.estimate_gas(tx)
        except Exception:
            logging.debug("⚠️ Gas estimation failed, using default gas limit.")

        signed_tx = web3.eth.account.sign_transaction(tx, private_key)
        tx_hash = web3.eth.send_raw_transaction(signed_tx.rawTransaction)
        logging.info(f"📤 Sent: {tx_hash.hex()} | {value} ETH | {from_address} → {to_address}")

        return wait_for_receipt(tx_hash)

    except ValueError as ve:
        logging.error(f"⚠️ Rejected: {ve} | {from_address} → {to_address}")
    except Exception as ex:
        logging.exception(f"💥 Error during transaction {from_address} → {to_address}: {ex}")
    return None


# --- Wallets I/O ---
def load_wallets(file_path: str) -> List[Dict[str, Any]]:
    """Load wallet list from JSON file with basic schema validation."""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        if not isinstance(data, list):
            raise ValueError("Wallet file must contain a list of dicts.")

        required_keys = {"from_address", "private_key", "to_address", "value"}
        valid_wallets: List[Dict[str, Any]] = []

        for entry in data:
            if isinstance(entry, dict) and required_keys.issubset(entry):
                try:
                    entry["value"] = float(entry["value"])
                    valid_wallets.append(entry)
                except ValueError:
                    logging.warning(f"⚠️ Invalid value type in wallet: {entry}")

        logging.info(f"🔑 Loaded {len(valid_wallets)} valid wallets from '{file_path}'")
        return valid_wallets

    except (FileNotFoundError, json.JSONDecodeError, ValueError) as err:
        logging.error(f"❌ Failed to load wallets: {err}")
        return []


def save_failed_wallets(wallets: List[Dict[str, Any]]) -> None:
    """Persist failed transactions for retry or audit."""
    if not wallets:
        return
    try:
        with open(FAILED_TX_FILE, "w", encoding="utf-8") as f:
            json.dump(wallets, f, indent=2)
        logging.info(f"💾 Saved {len(wallets)} failed transactions to '{FAILED_TX_FILE}'")
    except Exception as ex:
        logging.error(f"❌ Could not save failed transactions: {ex}")


# --- Worker Logic ---
def process_transaction(wallet: Dict[str, Any]) -> bool:
    """Process a single transaction. Returns True if success, False otherwise."""
    start = time.time()
    receipt = send_eth(
        from_address=wallet["from_address"],
        private_key=wallet["private_key"],
        to_address=wallet["to_address"],
        value=wallet["value"],
    )
    duration = time.time() - start
    if receipt:
        logging.info(f"⏱ Completed: {receipt['transactionHash'].hex()} in {duration:.2f}s")
        return True
    else:
        logging.warning(f"🚫 Failed: {wallet['from_address']} → {wallet['to_address']}")
        return False


def process_all_wallets(wallets: List[Dict[str, Any]]) -> None:
    if not wallets:
        logging.warning("⚠️ No wallets to process.")
        return

    logging.info(f"🚀 Processing {len(wallets)} transactions with {MAX_WORKERS} threads...")

    failed_wallets: List[Dict[str, Any]] = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_transaction, w): w for w in wallets}
        try:
            for future in concurrent.futures.as_completed(futures):
                wallet = futures[future]
                try:
                    if not future.result():
                        failed_wallets.append(wallet)
                except Exception as ex:
                    logging.error(f"❗ Exception for {wallet.get('from_address', 'unknown')}: {ex}")
                    failed_wallets.append(wallet)
        except KeyboardInterrupt:
            logging.warning("🛑 Interrupted by user. Shutting down...")
            executor.shutdown(wait=False, cancel_futures=True)
            save_failed_wallets(failed_wallets)
            sys.exit(1)

    save_failed_wallets(failed_wallets)


# --- Entrypoint ---
def main() -> None:
    try:
        wallets = load_wallets(WALLETS_FILE)
        if wallets:
            process_all_wallets(wallets)
        else:
            logging.error("🚫 No valid wallets loaded. Exiting.")
    except Exception as ex:
        logging.critical(f"Fatal error: {ex}")
        sys.exit(1)


if __name__ == "__main__":
    main()
