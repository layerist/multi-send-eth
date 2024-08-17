import concurrent.futures
from web3 import Web3
import json
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize Web3
infura_project_id = os.getenv('INFURA_PROJECT_ID')  # Use environment variable for security
infura_url = f"https://mainnet.infura.io/v3/{infura_project_id}"
web3 = Web3(Web3.HTTPProvider(infura_url))

if not web3.isConnected():
    raise ConnectionError("Unable to connect to the Ethereum network")

def send_eth(from_address, private_key, to_address, value):
    try:
        nonce = web3.eth.getTransactionCount(from_address)
        gas_price = web3.eth.gas_price

        # Create the transaction
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
        logging.info(f"Transaction sent: {tx_hash.hex()}")

        # Wait for the transaction receipt
        receipt = web3.eth.wait_for_transaction_receipt(tx_hash)
        return receipt

    except Exception as e:
        logging.error(f"Failed to send transaction from {from_address} to {to_address}: {str(e)}")
        return None

def load_wallets(file_path):
    try:
        with open(file_path) as f:
            wallets = json.load(f)
        return wallets
    except FileNotFoundError:
        logging.error("The wallets.json file was not found.")
        return []
    except json.JSONDecodeError:
        logging.error("Error decoding JSON from the wallets file.")
        return []

def handle_transaction(wallet):
    receipt = send_eth(
        from_address=wallet['from_address'],
        private_key=wallet['private_key'],
        to_address=wallet['to_address'],
        value=wallet['value']
    )

    if receipt:
        logging.info(f"Transaction successful with hash: {receipt.transactionHash.hex()}")
    else:
        logging.warning(f"Transaction failed for {wallet['from_address']} to {wallet['to_address']}.")

def process_wallets(wallets):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {executor.submit(handle_transaction, wallet): wallet for wallet in wallets}
        for future in concurrent.futures.as_completed(futures):
            wallet = futures[future]
            try:
                future.result()
            except Exception as e:
                logging.error(f"An error occurred for wallet {wallet['from_address']}: {str(e)}")

def main():
    wallets = load_wallets('wallets.json')
    if not wallets:
        logging.error("No wallets to process. Exiting.")
        return

    process_wallets(wallets)

if __name__ == "__main__":
    main()
