# Multi Send ETH

This Python script sends ETH from multiple source wallets to specified destination wallets concurrently using the `web3.py` library.

## Requirements

- Python 3.6+
- `web3.py`
- An Infura project ID

## Installation

1. Clone the repository:

    ```bash
    git clone https://github.com/layerist/multi-send-eth.git
    cd multi-send-eth
    ```

2. Install the required dependencies:

    ```bash
    pip install web3
    ```

3. Create a `wallets.json` file in the root directory of the project with the following structure:

    ```json
    [
        {
            "from_address": "0xYourSourceAddress1",
            "private_key": "YourPrivateKey1",
            "to_address": "0xDestinationAddress1",
            "value": 0.1
        },
        {
            "from_address": "0xYourSourceAddress2",
            "private_key": "YourPrivateKey2",
            "to_address": "0xDestinationAddress2",
            "value": 0.2
        }
    ]
    ```

4. Replace `"https://mainnet.infura.io/v3/YOUR_INFURA_PROJECT_ID"` in the script with your actual Infura project ID.

## Usage

Run the script using Python:

```bash
python multi_send_eth.py
```

The script will send ETH from the specified source wallets to the given destination wallets concurrently.

## Contributing

1. Fork the repository.
2. Create a new branch: `git checkout -b my-feature-branch`
3. Make your changes and commit them: `git commit -m 'Add some feature'`
4. Push to the branch: `git push origin my-feature-branch`
5. Submit a pull request.

## License

This project is licensed under the MIT License.
