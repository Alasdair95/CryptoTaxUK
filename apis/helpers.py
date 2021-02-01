

class Transaction:
    def __init__(self):
        self.transaction = {
            'asset': None,  # Which crypto
            'action': None,  # exchange_fiat_for_crypto, exchange_crypto_for_crypto, exchange_crypto_for_fiat, etc...
            'type': None,  # How Coinbase categorise the action
            'disposal': None,  # Bool - is the action considered a disposal by HMRC
            'datetime': None,  # Datetime of the action
            'initial_asset_quantity': None,  # Quantity of the initial asset in the action
            'initial_asset_currency': None,  # What asset does the action begin with
            'initial_asset_location': None,  # Where is the initial asset? wallet, exchange?
            'initial_asset_address': None,  # Wallet address or exchange wallet id
            'price': None,  # The exchange price if action is exchanging
            'final_asset_quantity': None,  # Quantity of the final asset in the action
            'final_asset_currency': None,  # What asset does the action end with
            'final_asset_gbp': None,  # The final GBP value at the time of the action
            'final_asset_location': None,  # Where is the final asset? wallet, exchange?
            'final_asset_address': None,  # Wallet address or exchange wallet id
            'fee_type': None,  # Exchange fee or transfer fee
            'fee_quantity': None,  # How much is the fee in the issued fee currency
            'fee_currency': None,  # The fee currency
            'fee_gbp': None,  # GBP value of the fee at the time of the action
            'source_transaction_id': None,  # Transaction id from the exchange/wallet where transaction occurred
            'source_trade_id': None  # Additional id field from the exchange/wallet to help match exchanges
        }
