import concurrent.futures
import json
import logging
import os
import time
from typing import List, Dict, Optional, Any

from web3 import Web3
from web3.exceptions import TransactionNotFound

# --- Configuration ---
DEFAULT_GAS_LIMIT = int(os.getenv("DEFAULT_GAS_LIMIT", "21000"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "5"))
RECEIPT_RETRY_DELAY = int(os.getenv("RECEIPT_RETRY_DELAY", "2"))
MAX_RECEIPT_RETRIES = int(os.getenv("MAX_RECEIPT_RETRIES", "5"))
WALLETS_FILE = os.getenv("WALLETS_FILE", "wallets.json")

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

# --- Helper Functions ---
def get_env_var(name: str) -> str:
    value = os.getenv(name)
    if not value:
        logging.critical(f"Missing environment variable: '{name}'")
        exit(1)
    return value

# --- Web3 Initialization ---
INFURA_PROJECT_ID = get_env_var("INFURA_PROJECT_ID")
INFURA_URL = f"https://mainnet.infura.io/v3/{INFURA_PROJECT_ID}"
web3 = Web3(Web3.HTTPProvider(INFURA_URL))

if not web3.is_connected():
    logging.critical("Unable to connect to Ethereum via Infura.")
    exit(1)

# --- Core Logic ---
def send_eth(from_address: str, private_key: str, to_address: str, value: float) -> Optional[Dict[str, Any]]:
    """
    Sends ETH from one address to another.
    """
    try:
        nonce = web3.eth.get_transaction_count(from_address)
        gas_price = web3.eth.gas_price or web3.to_wei('20', 'gwei')

        tx = {
            "nonce": nonce,
            "to": to_address,
            "value": web3.to_wei(value, "ether"),
            "gas": DEFAULT_GAS_LIMIT,
            "gasPrice": gas_price,
        }

        signed_tx = web3.eth.account.sign_transaction(tx, private_key)
        tx_hash = web3.eth.send_raw_transaction(signed_tx.rawTransaction)
        logging.info(f"📤 Sent: {tx_hash.hex()} | From: {from_address} → To: {to_address} | Value: {value} ETH")

        for attempt in range(1, MAX_RECEIPT_RETRIES + 1):
            try:
                receipt = web3.eth.get_transaction_receipt(tx_hash)
                logging.info(f"✅ Confirmed: {tx_hash.hex()} | Block: {receipt.blockNumber}")
                return receipt
            except TransactionNotFound:
                logging.debug(f"⌛ [{attempt}/{MAX_RECEIPT_RETRIES}] Awaiting confirmation: {tx_hash.hex()}")
                time.sleep(RECEIPT_RETRY_DELAY)

        logging.error(f"❌ Tx not confirmed after {MAX_RECEIPT_RETRIES} attempts: {tx_hash.hex()}")
    except ValueError as ve:
        logging.error(f"⚠️ Transaction rejected: {ve} | From: {from_address} → To: {to_address}")
    except Exception as ex:
        logging.exception(f"💥 Critical failure during transaction | From: {from_address} → To: {to_address} | Error: {ex}")
    return None


def load_wallets(file_path: str) -> List[Dict[str, Any]]:
    """
    Loads wallet transfer configurations from JSON file.
    """
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        if not isinstance(data, list):
            raise ValueError("Top-level JSON must be a list.")

        required_fields = {"from_address", "private_key", "to_address", "value"}
        valid_wallets = [w for w in data if required_fields.issubset(w)]

        if not valid_wallets:
            logging.warning(f"📂 No valid wallet entries in '{file_path}'")
        else:
            logging.info(f"🔑 Loaded {len(valid_wallets)} wallet entries from '{file_path}'")

        return valid_wallets

    except (FileNotFoundError, json.JSONDecodeError, ValueError) as err:
        logging.error(f"❌ Failed to load wallets: {err}")
        return []


def process_transaction(wallet: Dict[str, Any]) -> None:
    """
    Processes a single ETH transfer.
    """
    start_time = time.time()
    try:
        receipt = send_eth(
            from_address=wallet["from_address"],
            private_key=wallet["private_key"],
            to_address=wallet["to_address"],
            value=wallet["value"]
        )
        if receipt:
            elapsed = time.time() - start_time
            logging.info(f"⏱ Success: {receipt['transactionHash'].hex()} | Time: {elapsed:.2f}s")
        else:
            logging.warning(f"🚫 Transfer failed: {wallet['from_address']} → {wallet['to_address']}")
    except Exception as e:
        logging.exception(f"❗ Exception during processing | Wallet: {wallet}")


def process_all_wallets(wallets: List[Dict[str, Any]]) -> None:
    """
    Processes multiple wallet transfers concurrently.
    """
    if not wallets:
        logging.warning("⚠️ No wallet data to process.")
        return

    logging.info(f"🚀 Starting {len(wallets)} transactions with max {MAX_WORKERS} threads.")

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_map = {executor.submit(process_transaction, wallet): wallet for wallet in wallets}

        for future in concurrent.futures.as_completed(future_map):
            try:
                future.result()
            except Exception as e:
                wallet = future_map[future]
                logging.error(f"❗ Thread error for {wallet.get('from_address', 'unknown')}: {e}")


def main() -> None:
    """
    Entry point for the script.
    """
    wallets = load_wallets(WALLETS_FILE)
    if wallets:
        process_all_wallets(wallets)
    else:
        logging.error("🚫 No valid wallets loaded. Exiting.")


if __name__ == "__main__":
    main()
