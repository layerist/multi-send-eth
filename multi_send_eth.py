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

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)

# Web3 setup
INFURA_PROJECT_ID = os.getenv("INFURA_PROJECT_ID")
if not INFURA_PROJECT_ID:
    logging.critical("INFURA_PROJECT_ID environment variable is not set. Exiting.")
    exit(1)

INFURA_URL = f"https://mainnet.infura.io/v3/{INFURA_PROJECT_ID}"
web3 = Web3(Web3.HTTPProvider(INFURA_URL))

if not web3.is_connected():
    logging.critical("Failed to connect to Ethereum network. Exiting.")
    exit(1)


def send_eth(from_address: str, private_key: str, to_address: str, value: float) -> Optional[Dict[str, Any]]:
    try:
        nonce = web3.eth.get_transaction_count(from_address)
        gas_price = web3.eth.gas_price

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

        logging.error(f"Transaction {tx_hash.hex()} not confirmed after {MAX_RECEIPT_RETRIES} retries.")
        return None

    except ValueError as e:
        logging.error(f"ValueError for transaction {from_address} -> {to_address}: {e}")
    except Exception as e:
        logging.exception(f"Unexpected error for transaction {from_address} -> {to_address}: {e}")

    return None


def load_wallets(file_path: str) -> List[Dict[str, Any]]:
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            wallets = json.load(file)

        if not isinstance(wallets, list):
            raise ValueError("Expected a list of wallet dictionaries.")

        valid_wallets = [
            w for w in wallets if {"from_address", "private_key", "to_address", "value"}.issubset(w)
        ]

        if not valid_wallets:
            logging.warning(f"No valid wallets found in {file_path}.")
        else:
            logging.info(f"Loaded {len(valid_wallets)} wallets from {file_path}.")

        return valid_wallets

    except (FileNotFoundError, json.JSONDecodeError, ValueError) as e:
        logging.error(f"Failed to load wallets: {e}")
        return []


def process_transaction(wallet: Dict[str, Any]) -> None:
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
        logging.exception(f"Exception during transaction: {wallet.get('from_address')} -> {wallet.get('to_address')}")


def process_all_wallets(wallets: List[Dict[str, Any]]) -> None:
    if not wallets:
        logging.warning("No wallets to process.")
        return

    logging.info(f"Starting {len(wallets)} transactions using up to {MAX_WORKERS} workers.")

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_transaction, wallet): wallet for wallet in wallets}

        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                wallet = futures[future]
                logging.error(f"Unhandled error for wallet {wallet.get('from_address', 'unknown')}: {e}")


def main() -> None:
    wallets = load_wallets("wallets.json")
    if wallets:
        process_all_wallets(wallets)
    else:
        logging.error("No valid wallets to process. Exiting.")


if __name__ == "__main__":
    main()
