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
RECEIPT_RETRY_DELAY = 2
MAX_RECEIPT_RETRIES = 5

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
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
    logging.critical("Unable to connect to Ethereum network via Infura. Exiting.")
    exit(1)


def send_eth(from_address: str, private_key: str, to_address: str, value: float) -> Optional[Dict[str, Any]]:
    """
    Sends ETH from one address to another.
    """
    try:
        nonce = web3.eth.get_transaction_count(from_address)
        gas_price = web3.eth.gas_price or web3.to_wei('20', 'gwei')

        transaction = {
            "nonce": nonce,
            "to": to_address,
            "value": web3.to_wei(value, "ether"),
            "gas": DEFAULT_GAS_LIMIT,
            "gasPrice": gas_price,
        }

        signed_tx = web3.eth.account.sign_transaction(transaction, private_key)
        tx_hash = web3.eth.send_raw_transaction(signed_tx.rawTransaction)
        logging.info(f"Transaction sent: {tx_hash.hex()}")

        for attempt in range(1, MAX_RECEIPT_RETRIES + 1):
            try:
                receipt = web3.eth.get_transaction_receipt(tx_hash)
                logging.info(f"Transaction confirmed: {tx_hash.hex()} | Block: {receipt['blockNumber']}")
                return receipt
            except TransactionNotFound:
                logging.debug(f"[{attempt}/{MAX_RECEIPT_RETRIES}] Waiting for transaction {tx_hash.hex()} to be mined...")
                time.sleep(RECEIPT_RETRY_DELAY)

        logging.error(f"Transaction {tx_hash.hex()} was not confirmed after {MAX_RECEIPT_RETRIES} attempts.")
        return None

    except ValueError as e:
        logging.error(f"ValueError: {e} | From: {from_address} -> To: {to_address}")
    except Exception as e:
        logging.exception(f"Unexpected error: {e} | From: {from_address} -> To: {to_address}")
    return None


def load_wallets(file_path: str) -> List[Dict[str, Any]]:
    """
    Loads wallet data from a JSON file.
    """
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            wallets = json.load(file)

        if not isinstance(wallets, list):
            raise ValueError("JSON root must be a list of wallet entries.")

        required_keys = {"from_address", "private_key", "to_address", "value"}
        valid_wallets = [w for w in wallets if required_keys.issubset(w)]

        if not valid_wallets:
            logging.warning(f"No valid wallet entries found in '{file_path}'.")
        else:
            logging.info(f"Loaded {len(valid_wallets)} wallets from '{file_path}'.")

        return valid_wallets

    except (FileNotFoundError, json.JSONDecodeError, ValueError) as e:
        logging.error(f"Error loading wallet file '{file_path}': {e}")
        return []


def process_transaction(wallet: Dict[str, Any]) -> None:
    """
    Processes a single ETH transfer transaction.
    """
    try:
        receipt = send_eth(
            from_address=wallet["from_address"],
            private_key=wallet["private_key"],
            to_address=wallet["to_address"],
            value=wallet["value"],
        )
        if receipt:
            logging.info(f"✅ Success: {receipt['transactionHash'].hex()}")
        else:
            logging.warning(f"❌ Failed: {wallet['from_address']} -> {wallet['to_address']}")
    except Exception as e:
        logging.exception(f"Unhandled exception: {e} | Wallet: {wallet}")


def process_all_wallets(wallets: List[Dict[str, Any]]) -> None:
    """
    Executes ETH transfers concurrently using a thread pool.
    """
    if not wallets:
        logging.warning("No wallets to process.")
        return

    logging.info(f"Starting processing of {len(wallets)} transactions with up to {MAX_WORKERS} threads.")

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_transaction, wallet): wallet for wallet in wallets}

        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                wallet = futures[future]
                logging.error(f"Unhandled error for wallet {wallet.get('from_address', 'unknown')}: {e}")


def main() -> None:
    """
    Entry point of the script.
    """
    wallets = load_wallets("wallets.json")
    if wallets:
        process_all_wallets(wallets)
    else:
        logging.error("No valid wallets found. Exiting.")


if __name__ == "__main__":
    main()
