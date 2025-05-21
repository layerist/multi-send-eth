import concurrent.futures
import json
import logging
import os
import time
from typing import List, Dict, Optional, Any

from web3 import Web3
from web3.exceptions import TransactionNotFound

# Configuration
DEFAULT_GAS_LIMIT = int(os.getenv("DEFAULT_GAS_LIMIT", 21000))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", 5))
RECEIPT_RETRY_DELAY = int(os.getenv("RECEIPT_RETRY_DELAY", 2))
MAX_RECEIPT_RETRIES = int(os.getenv("MAX_RECEIPT_RETRIES", 5))
WALLETS_FILE = os.getenv("WALLETS_FILE", "wallets.json")

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)


def get_env_var(name: str) -> str:
    value = os.getenv(name)
    if not value:
        logging.critical(f"Environment variable '{name}' is not set. Exiting.")
        exit(1)
    return value


# Web3 initialization
INFURA_PROJECT_ID = get_env_var("INFURA_PROJECT_ID")
INFURA_URL = f"https://mainnet.infura.io/v3/{INFURA_PROJECT_ID}"
web3 = Web3(Web3.HTTPProvider(INFURA_URL))

if not web3.is_connected():
    logging.critical("Failed to connect to Ethereum via Infura. Exiting.")
    exit(1)


def send_eth(from_address: str, private_key: str, to_address: str, value: float) -> Optional[Dict[str, Any]]:
    """
    Sends ETH from one address to another.
    """
    try:
        nonce = web3.eth.get_transaction_count(from_address)
        gas_price = web3.eth.gas_price or web3.to_wei('20', 'gwei')  # Fallback to static value

        tx = {
            "nonce": nonce,
            "to": to_address,
            "value": web3.to_wei(value, "ether"),
            "gas": DEFAULT_GAS_LIMIT,
            "gasPrice": gas_price,
        }

        signed_tx = web3.eth.account.sign_transaction(tx, private_key)
        tx_hash = web3.eth.send_raw_transaction(signed_tx.rawTransaction)
        logging.info(f"📤 Transaction sent: {tx_hash.hex()} | From: {from_address} → To: {to_address}")

        for attempt in range(1, MAX_RECEIPT_RETRIES + 1):
            try:
                receipt = web3.eth.get_transaction_receipt(tx_hash)
                logging.info(f"✅ Confirmed: {tx_hash.hex()} | Block: {receipt.blockNumber}")
                return receipt
            except TransactionNotFound:
                logging.debug(f"[Attempt {attempt}/{MAX_RECEIPT_RETRIES}] Waiting for confirmation of {tx_hash.hex()}")
                time.sleep(RECEIPT_RETRY_DELAY)

        logging.error(f"❌ Unconfirmed after {MAX_RECEIPT_RETRIES} retries: {tx_hash.hex()}")
        return None

    except ValueError as e:
        logging.error(f"⚠️ ValueError: {e} | From: {from_address} → To: {to_address}")
    except Exception as e:
        logging.exception(f"💥 Unexpected error: {e} | From: {from_address} → To: {to_address}")
    return None


def load_wallets(file_path: str) -> List[Dict[str, Any]]:
    """
    Loads wallet data from a JSON file.
    """
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            wallets = json.load(file)

        if not isinstance(wallets, list):
            raise ValueError("Root JSON must be a list of wallet objects.")

        required = {"from_address", "private_key", "to_address", "value"}
        valid_wallets = [w for w in wallets if required.issubset(w)]

        if not valid_wallets:
            logging.warning(f"No valid wallets found in '{file_path}'.")
        else:
            logging.info(f"🔑 Loaded {len(valid_wallets)} wallets from '{file_path}'.")

        return valid_wallets

    except (FileNotFoundError, json.JSONDecodeError, ValueError) as e:
        logging.error(f"❌ Failed to load wallets from '{file_path}': {e}")
        return []


def process_transaction(wallet: Dict[str, Any]) -> None:
    """
    Executes a single ETH transaction.
    """
    start = time.time()
    try:
        receipt = send_eth(
            from_address=wallet["from_address"],
            private_key=wallet["private_key"],
            to_address=wallet["to_address"],
            value=wallet["value"],
        )
        if receipt:
            duration = time.time() - start
            logging.info(f"⏱ Completed: {receipt['transactionHash'].hex()} in {duration:.2f}s")
        else:
            logging.warning(f"🚫 Failed: {wallet['from_address']} → {wallet['to_address']}")
    except Exception as e:
        logging.exception(f"❗ Unhandled error during transaction | Wallet: {wallet}")


def process_all_wallets(wallets: List[Dict[str, Any]]) -> None:
    """
    Executes multiple ETH transfers concurrently.
    """
    if not wallets:
        logging.warning("⚠️ No wallets to process.")
        return

    logging.info(f"🚀 Processing {len(wallets)} transactions with up to {MAX_WORKERS} threads.")

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_transaction, w): w for w in wallets}

        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                wallet = futures[future]
                logging.error(f"❗ Error in thread for {wallet.get('from_address', 'unknown')}: {e}")


def main() -> None:
    """
    Main entry point.
    """
    wallets = load_wallets(WALLETS_FILE)
    if wallets:
        process_all_wallets(wallets)
    else:
        logging.error("🚫 No valid wallet entries. Terminating.")


if __name__ == "__main__":
    main()
