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
RECEIPT_RETRY_DELAY = 2  # Seconds to wait before retrying receipt fetch
MAX_RECEIPT_RETRIES = 5  # Maximum retries for transaction receipt

# Logging setup
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)

# Infura Web3 connection
INFURA_PROJECT_ID = os.getenv("INFURA_PROJECT_ID")
if not INFURA_PROJECT_ID:
    logging.critical("INFURA_PROJECT_ID environment variable is not set. Exiting.")
    exit(1)

INFURA_URL = f"https://mainnet.infura.io/v3/{INFURA_PROJECT_ID}"


def get_web3() -> Web3:
    """Lazy-initialized Web3 instance."""
    web3_instance = Web3(Web3.HTTPProvider(INFURA_URL))
    if not web3_instance.is_connected():
        logging.critical("Failed to connect to Ethereum network. Exiting.")
        exit(1)
    return web3_instance


web3 = get_web3()


def send_eth(from_address: str, private_key: str, to_address: str, value: float) -> Optional[Dict[str, Any]]:
    """
    Send ETH from one address to another with error handling.
    """
    try:
        nonce = web3.eth.get_transaction_count(from_address)
        gas_price = web3.eth.gas_price

        tx = {
            "nonce": nonce,
            "to": to_address,
            "value": web3.to_wei(value, "ether"),
            "gas": DEFAULT_GAS_LIMIT,
            "gasPrice": gas_price,
        }

        signed_tx = web3.eth.account.sign_transaction(tx, private_key)
        tx_hash = web3.eth.send_raw_transaction(signed_tx.rawTransaction)
        logging.info(f"Transaction sent. Hash: {tx_hash.hex()}")

        # Attempt to fetch receipt with retries
        for attempt in range(MAX_RECEIPT_RETRIES):
            try:
                receipt = web3.eth.get_transaction_receipt(tx_hash)
                logging.info(f"Transaction confirmed. Hash: {tx_hash.hex()} | Block: {receipt['blockNumber']}")
                return receipt
            except TransactionNotFound:
                logging.debug(f"Waiting for transaction {tx_hash.hex()} to be mined... (Attempt {attempt+1}/{MAX_RECEIPT_RETRIES})")
                time.sleep(RECEIPT_RETRY_DELAY)

        logging.error(f"Transaction {tx_hash.hex()} not found after retries.")
        return None

    except ValueError as e:
        logging.error(f"ValueError while sending ETH from {from_address} to {to_address}: {e}")
    except Exception as e:
        logging.error(f"Unexpected error while sending ETH from {from_address} to {to_address}: {e}")

    return None


def load_wallets(file_path: str) -> List[Dict[str, Any]]:
    """Load and validate wallet information from a JSON file."""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            wallets = json.load(f)

        if not isinstance(wallets, list):
            raise ValueError("Invalid format: JSON file must contain a list of wallets.")

        valid_wallets = [
            wallet for wallet in wallets
            if {"from_address", "private_key", "to_address", "value"}.issubset(wallet)
        ]

        if not valid_wallets:
            logging.warning(f"No valid wallets found in {file_path}. Ensure correct JSON structure.")

        logging.info(f"Loaded {len(valid_wallets)} valid wallets from {file_path}")
        return valid_wallets

    except (FileNotFoundError, json.JSONDecodeError, ValueError) as e:
        logging.error(f"Error loading wallets from {file_path}: {e}")
    return []


def handle_transaction(wallet: Dict[str, Any]) -> None:
    """Process a single transaction with error handling."""
    try:
        receipt = send_eth(
            from_address=wallet["from_address"],
            private_key=wallet["private_key"],
            to_address=wallet["to_address"],
            value=wallet["value"],
        )
        if receipt:
            logging.info(f"Transaction successful: {receipt['transactionHash'].hex()}")
        else:
            logging.warning(f"Transaction failed: {wallet['from_address']} -> {wallet['to_address']}")
    except Exception as e:
        logging.error(f"Error processing transaction from {wallet['from_address']}: {e}")


def process_wallets(wallets: List[Dict[str, Any]]) -> None:
    """Process multiple wallet transactions concurrently."""
    if not wallets:
        logging.warning("No wallets to process.")
        return

    logging.info(f"Processing {len(wallets)} transactions with up to {MAX_WORKERS} workers.")
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(handle_transaction, wallet): wallet for wallet in wallets}
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                wallet = futures[future]
                logging.error(f"Error handling transaction for wallet {wallet.get('from_address', 'unknown')}: {e}")


def main() -> None:
    """Main function to load wallets and process transactions."""
    wallets = load_wallets("wallets.json")
    if not wallets:
        logging.error("No valid wallets found. Exiting.")
        return

    process_wallets(wallets)


if __name__ == "__main__":
    main()
