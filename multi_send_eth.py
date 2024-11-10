import concurrent.futures
from web3 import Web3
import json
import logging
import os
from typing import List, Dict, Optional

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize Web3 connection using Infura
infura_project_id = os.getenv('INFURA_PROJECT_ID')
if not infura_project_id:
    logging.critical("INFURA_PROJECT_ID environment variable is not set.")
    exit(1)

infura_url = f"https://mainnet.infura.io/v3/{infura_project_id}"
web3 = Web3(Web3.HTTPProvider(infura_url))

if not web3.isConnected():
    logging.critical("Unable to connect to the Ethereum network.")
    exit(1)

def send_eth(from_address: str, private_key: str, to_address: str, value: float) -> Optional[Dict]:
    """Send ETH from one address to another."""
    try:
        nonce = web3.eth.getTransactionCount(from_address)
        gas_price = web3.eth.gas_price

        tx = {
            'nonce': nonce,
            'to': to_address,
            'value': web3.toWei(value, 'ether'),
            'gas': 21000,
            'gasPrice': gas_price,
        }

        # Sign and send the transaction
        signed_tx = web3.eth.account.sign_transaction(tx, private_key)
        tx_hash = web3.eth.send_raw_transaction(signed_tx.rawTransaction)
        logging.info(f"Transaction sent with hash: {tx_hash.hex()}")

        # Wait for the transaction receipt
        receipt = web3.eth.wait_for_transaction_receipt(tx_hash)
        logging.info(f"Transaction confirmed. Receipt: {receipt}")
        return receipt

    except Exception as e:
        logging.error(f"Error sending transaction from {from_address} to {to_address}: {e}")
        return None

def load_wallets(file_path: str) -> List[Dict]:
    """Load wallet information from a JSON file."""
    try:
        with open(file_path, 'r') as f:
            wallets = json.load(f)
        logging.debug(f"Loaded {len(wallets)} wallets from {file_path}")
        return wallets
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logging.error(f"Error loading wallets from {file_path}: {e}")
        return []

def handle_transaction(wallet: Dict):
    """Handle a single ETH transaction."""
    try:
        logging.debug(f"Processing transaction from {wallet['from_address']} to {wallet['to_address']}")
        receipt = send_eth(
            from_address=wallet['from_address'],
            private_key=wallet['private_key'],
            to_address=wallet['to_address'],
            value=wallet['value']
        )
        if receipt:
            logging.info(f"Transaction successful. Hash: {receipt.transactionHash.hex()}")
        else:
            logging.warning(f"Transaction failed for {wallet['from_address']} to {wallet['to_address']}.")
    except KeyError as e:
        logging.error(f"Missing key in wallet data: {e}")
    except Exception as e:
        logging.error(f"Error processing transaction for wallet {wallet['from_address']}: {e}")

def process_wallets(wallets: List[Dict]):
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
                logging.error(f"Error handling transaction for wallet {wallet['from_address']}: {e}")

def main():
    wallets = load_wallets('wallets.json')
    if not wallets:
        logging.error("No valid wallets found. Exiting.")
        return

    process_wallets(wallets)

if __name__ == "__main__":
    main()
