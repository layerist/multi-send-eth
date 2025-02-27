import concurrent.futures
import json
import logging
import os
from typing import List, Dict, Optional, Any
from web3 import Web3
from web3.exceptions import TransactionNotFound

# Constants
DEFAULT_GAS_LIMIT = 21000

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)

# Initialize Web3 connection using Infura
INFURA_PROJECT_ID = os.getenv("INFURA_PROJECT_ID")
if not INFURA_PROJECT_ID:
    logging.critical("INFURA_PROJECT_ID environment variable is not set. Exiting.")
    exit(1)

INFURA_URL = f"https://mainnet.infura.io/v3/{INFURA_PROJECT_ID}"


def init_web3(provider_url: str) -> Web3:
    """Initialize and return a Web3 instance."""
    web3_instance = Web3(Web3.HTTPProvider(provider_url))
    if not web3_instance.is_connected():
        logging.critical("Unable to connect to the Ethereum network. Exiting.")
        exit(1)
    return web3_instance


web3 = init_web3(INFURA_URL)


def send_eth(from_address: str, private_key: str, to_address: str, value: float) -> Optional[Dict[str, Any]]:
    """
    Send ETH from one address to another.
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

        receipt = web3.eth.wait_for_transaction_receipt(tx_hash)
        logging.info(f"Transaction confirmed. Receipt: {receipt}")
        return receipt

    except ValueError as e:
        logging.error(f"ValueError while sending ETH: {e}")
    except TransactionNotFound:
        logging.error("Transaction not found after broadcasting.")
    except Exception as e:
        logging.error(f"Unexpected error while sending ETH: {e}")
    
    return None


def load_wallets(file_path: str) -> List[Dict[str, Any]]:
    """Load wallet information from a JSON file."""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            wallets = json.load(f)
        
        if not isinstance(wallets, list):
            raise ValueError("JSON file does not contain a list of wallets.")
        
        logging.debug(f"Loaded {len(wallets)} wallets from {file_path}")
        return wallets
    except (FileNotFoundError, json.JSONDecodeError, ValueError) as e:
        logging.error(f"Error loading wallets from {file_path}: {e}")
    return []


def handle_transaction(wallet: Dict[str, Any]) -> None:
    """Handle a single ETH transaction."""
    try:
        required_keys = {"from_address", "private_key", "to_address", "value"}
        if not required_keys.issubset(wallet):
            missing_keys = required_keys - wallet.keys()
            logging.error(f"Missing required keys in wallet data: {missing_keys}")
            return
        
        logging.debug(f"Processing transaction from {wallet['from_address']} to {wallet['to_address']}")
        receipt = send_eth(
            from_address=wallet["from_address"],
            private_key=wallet["private_key"],
            to_address=wallet["to_address"],
            value=wallet["value"],
        )
        if receipt:
            logging.info(f"Transaction successful. Hash: {receipt['transactionHash'].hex()}")
        else:
            logging.warning(f"Transaction failed for {wallet['from_address']} to {wallet['to_address']}")
    except Exception as e:
        logging.error(f"Error processing transaction for {wallet.get('from_address', 'unknown')}: {e}")


def process_wallets(wallets: List[Dict[str, Any]]) -> None:
    """Process a list of wallet transactions concurrently."""
    if not wallets:
        logging.warning("No wallets provided for processing.")
        return

    logging.info(f"Starting to process {len(wallets)} wallet transactions concurrently.")
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {executor.submit(handle_transaction, wallet): wallet for wallet in wallets}
        for future in concurrent.futures.as_completed(futures):
            wallet = futures[future]
            try:
                future.result()
            except Exception as e:
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
