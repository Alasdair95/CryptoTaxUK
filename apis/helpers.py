import os
import requests

from apis.authentication import CoinbaseProAuth


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


class ConvertToGBP:
    def __init__(self, asset, dt, quantity):
        self.asset = asset
        self.dt = dt
        self.quantity = quantity
        self.api_key = os.environ.get('COINBASE_PRO_API_KEY')
        self.api_secret = os.environ.get('COINBASE_PRO_API_SECRET')
        self.passphrase = os.environ.get('COINBASE_PRO_API_PASSPHRASE')
        self.base_url = 'https://api.pro.coinbase.com/'

    def convert_to_gbp(self):
        quantity_btc = round(self.quantity * self.get_historical_crypto_btc_price(), 8)

        # Must first convert to USD because most exchanges only have recent GBP prices but will have historical USD
        quantity_usd = round(quantity_btc * self.get_historical_btc_usd_price(), 2)

        quantity_gbp = round(quantity_usd * self.get_historical_usd_gbp_price(), 2)

        return quantity_gbp

    def get_historical_crypto_btc_price(self):
        path = self.base_url + f'products/{self.asset}-BTC/candles'
        params = {
            'start': f'{self.dt}',
            'end': f'{self.dt}',
            'granularity': 60
        }
        auth = CoinbaseProAuth(self.api_key, self.api_secret, self.passphrase)
        r = requests.get(path, auth=auth, params=params).json()

        return r[0][4]

    def get_historical_btc_usd_price(self):
        path = self.base_url + f'products/BTC-USD/candles'
        params = {
            'start': f'{self.dt}',
            'end': f'{self.dt}',
            'granularity': 60
        }
        auth = CoinbaseProAuth(self.api_key, self.api_secret, self.passphrase)
        r = requests.get(path, auth=auth, params=params).json()

        return r[0][4]

    def get_historical_usd_gbp_price(self):
        url = f'https://api.ratesapi.io/api/{self.dt.split("T")[0]}?base=USD&symbols=USD,GBP'

        r = requests.get(url)

        if r.status_code == 200:
            return r.json()['rates']['GBP']


if __name__ == '__main__':
    x = ConvertToGBP('NMR', '2021-03-13T13:54:00', 0.069)
    x.convert_to_gbp()

