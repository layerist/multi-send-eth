import threading
from web3 import Web3
import json

# Initialize Web3
infura_url = "https://mainnet.infura.io/v3/YOUR_INFURA_PROJECT_ID"
web3 = Web3(Web3.HTTPProvider(infura_url))

if not web3.isConnected():
    raise ConnectionError("Unable to connect to the Ethereum network")

# Function to send ETH
def send_eth(from_address, private_key, to_address, value):
    nonce = web3.eth.getTransactionCount(from_address)
    gas_price = web3.eth.gas_price

    tx = {
        'nonce': nonce,
        'to': to_address,
        'value': web3.toWei(value, 'ether'),
        'gas': 21000,
        'gasPrice': gas_price,
    }

    signed_tx = web3.eth.account.sign_transaction(tx, private_key)
    tx_hash = web3.eth.send_raw_transaction(signed_tx.rawTransaction)
    print(f"Transaction sent: {tx_hash.hex()}")
    receipt = web3.eth.wait_for_transaction_receipt(tx_hash)
    return receipt

# Load wallet data
with open('wallets.json') as f:
    wallets = json.load(f)

# Function to handle sending ETH in threads
def handle_transaction(wallet):
    try:
        receipt = send_eth(wallet['from_address'], wallet['private_key'], wallet['to_address'], wallet['value'])
        print(f"Transaction successful with hash: {receipt.transactionHash.hex()}")
    except Exception as e:
        print(f"Transaction failed for {wallet['from_address']} to {wallet['to_address']}: {str(e)}")

# Main function
def main():
    threads = []

    for wallet in wallets:
        thread = threading.Thread(target=handle_transaction, args=(wallet,))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

if __name__ == "__main__":
    main()
