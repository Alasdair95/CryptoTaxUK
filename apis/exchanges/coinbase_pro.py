import os
import re
import requests
import pandas as pd

from apis.authentication import CoinbaseProAuth
from apis.helpers import Transaction


class CoinbasePro:
    def __init__(self):
        self.api_key = os.environ.get('COINBASE_PRO_API_KEY')
        self.api_secret = os.environ.get('COINBASE_PRO_API_SECRET')
        self.passphrase = os.environ.get('COINBASE_PRO_API_PASSPHRASE')
        self.base_url = 'https://api.pro.coinbase.com/'

    def get_coinbase_pro_transactions(self):
        """
        Method to execute all other methods in correct order to return all historical transactions from Coinbase.
        """

        # Pseudo-code:
        # - Get list of all trading pairs
        # - Loop over this list and call the fills endpoint. If there have ever been any trades then keep the pair
        # - Loop through pair with trade and save the trade history
        # - Loop through trade history and create my transaction dataframe

        # Get list of products
        products = self.get_products()

        # Loop through products and keep products with transactions
        all_fills = []
        for product_id in products:
            transactions = self.transactions_exist(product_id)
            if transactions:
                all_fills = all_fills + transactions

        # Convert all transactions into pandas dataframes
        all_fills_dataframes = []
        for fill in all_fills:
            all_fills_dataframes.append(CoinbasePro.create_fills_dataframe(fill))

        df_fills = pd.concat(all_fills_dataframes).sort_values(by='datetime')

        # Get all account ids and their corresponding assets
        accounts = self.get_accounts()

        all_depostits = self.get_deposits(accounts)

        all_withdrawals = self.get_withdrawals(accounts)

        return None

    def get_products(self):
        """
        Get list of products. Products are currency pairs (BTC-EUR for example).
        """

        # Set up authentication and make call to the API
        path = self.base_url + 'products'
        auth = CoinbaseProAuth(self.api_key, self.api_secret, self.passphrase)
        r = requests.get(path, auth=auth).json()

        # Create list of product ids
        products = [i['id'] for i in r]

        return products

    def get_accounts(self):
        path = self.base_url + 'accounts'

        # Set up authentication and make call to the API
        auth = CoinbaseProAuth(self.api_key, self.api_secret, self.passphrase)
        r = requests.get(path, auth=auth).json()

        accounts = {i['id']: i['currency'] for i in r}

        return accounts

    def transactions_exist(self, product_id):
        """
        Function to return True if there has ever been a transaction for a given product id
        """

        # Set up authentication and make call to the API
        path = self.base_url + 'fills'
        params = {'product_id': f'{product_id}'}
        auth = CoinbaseProAuth(self.api_key, self.api_secret, self.passphrase)
        r = requests.get(path, auth=auth, params=params).json()

        if len(r) > 0:
            return r

    @staticmethod
    def create_fills_dataframe(fill):
        if fill['product_id'].split('-')[1] in ['GBP', 'EUR']:
            side = fill['side']

            if side == 'buy':
                tx = Transaction()

                tx.transaction['asset'] = fill['product_id'].split('-')[0]
                tx.transaction['action'] = 'exchange_fiat_for_crypto'
                tx.transaction['disposal'] = False
                tx.transaction['datetime'] = fill['created_at']
                tx.transaction['initial_asset_quantity'] = float(fill['price']) * float(fill['size'])
                tx.transaction['initial_asset_currency'] = fill['product_id'].split('-')[1]
                tx.transaction['initial_asset_location'] = 'Coinbase Pro'
                tx.transaction['initial_asset_address'] = None
                tx.transaction['price'] = fill['price']
                tx.transaction['final_asset_quantity'] = fill['size']
                tx.transaction['final_asset_currency'] = fill['product_id'].split('-')[0]
                tx.transaction['final_asset_gbp'] = None
                tx.transaction['final_asset_location'] = 'Coinbase Pro'
                tx.transaction['final_asset_address'] = None
                tx.transaction['fee_type'] = 'exchange'
                tx.transaction['fee_quantity'] = fill['fee']
                tx.transaction['fee_currency'] = fill['product_id'].split('-')[1]
                tx.transaction['fee_gbp'] = None
                tx.transaction['source_transaction_id'] = fill['order_id']
                tx.transaction['source_trade_id'] = fill['trade_id']

                return pd.DataFrame(tx.transaction, index=[0])

            else:
                tx = Transaction()

                tx.transaction['asset'] = fill['product_id'].split('-')[0]
                tx.transaction['action'] = 'exchange_crypto_for_fiat'
                tx.transaction['disposal'] = True
                tx.transaction['datetime'] = fill['created_at']
                tx.transaction['initial_asset_quantity'] = float(fill['size'])
                tx.transaction['initial_asset_currency'] = fill['product_id'].split('-')[0]
                tx.transaction['initial_asset_location'] = 'Coinbase Pro'
                tx.transaction['initial_asset_address'] = None
                tx.transaction['price'] = fill['price']
                tx.transaction['final_asset_quantity'] = fill['size']
                tx.transaction['final_asset_currency'] = fill['product_id'].split('-')[1]
                tx.transaction['final_asset_gbp'] = None
                tx.transaction['final_asset_location'] = 'Coinbase Pro'
                tx.transaction['final_asset_address'] = None
                tx.transaction['fee_type'] = 'exchange'
                tx.transaction['fee_quantity'] = fill['fee']
                tx.transaction['fee_currency'] = fill['product_id'].split('-')[1]
                tx.transaction['fee_gbp'] = None
                tx.transaction['source_transaction_id'] = fill['order_id']
                tx.transaction['source_trade_id'] = fill['trade_id']

                return pd.DataFrame(tx.transaction, index=[0])

        else:
            side = fill['side']

            if side == 'sell':
                sell_tx = Transaction()

                sell_tx.transaction['asset'] = fill['product_id'].split('-')[0]
                sell_tx.transaction['action'] = 'exchange_crypto_for_crypto'
                sell_tx.transaction['disposal'] = True
                sell_tx.transaction['datetime'] = fill['created_at']
                sell_tx.transaction['initial_asset_quantity'] = float(fill['price']) * float(fill['size'])
                sell_tx.transaction['initial_asset_currency'] = fill['product_id'].split('-')[0]
                sell_tx.transaction['initial_asset_location'] = 'Coinbase Pro'
                sell_tx.transaction['initial_asset_address'] = None
                sell_tx.transaction['price'] = fill['price']
                sell_tx.transaction['final_asset_quantity'] = fill['size']
                sell_tx.transaction['final_asset_currency'] = fill['product_id'].split('-')[1]
                sell_tx.transaction['final_asset_gbp'] = None
                sell_tx.transaction['final_asset_location'] = 'Coinbase Pro'
                sell_tx.transaction['final_asset_address'] = None
                sell_tx.transaction['fee_type'] = 'exchange'
                sell_tx.transaction['fee_quantity'] = fill['fee']
                sell_tx.transaction['fee_currency'] = fill['product_id'].split('-')[1]
                sell_tx.transaction['fee_gbp'] = None
                sell_tx.transaction['source_transaction_id'] = fill['order_id']
                sell_tx.transaction['source_trade_id'] = fill['trade_id']

                sell_df = pd.DataFrame(sell_tx.transaction, index=[0])

                buy_tx = Transaction()

                buy_tx.transaction['asset'] = fill['product_id'].split('-')[1]
                buy_tx.transaction['action'] = 'exchange_crypto_for_crypto'
                buy_tx.transaction['disposal'] = False
                buy_tx.transaction['datetime'] = fill['created_at']
                buy_tx.transaction['initial_asset_quantity'] = float(fill['price']) * float(fill['size'])
                buy_tx.transaction['initial_asset_currency'] = fill['product_id'].split('-')[0]
                buy_tx.transaction['initial_asset_location'] = 'Coinbase Pro'
                buy_tx.transaction['initial_asset_address'] = None
                buy_tx.transaction['price'] = fill['price']
                buy_tx.transaction['final_asset_quantity'] = fill['size']
                buy_tx.transaction['final_asset_currency'] = fill['product_id'].split('-')[1]
                buy_tx.transaction['final_asset_gbp'] = None
                buy_tx.transaction['final_asset_location'] = 'Coinbase Pro'
                buy_tx.transaction['final_asset_address'] = None
                buy_tx.transaction['fee_type'] = 'exchange'
                buy_tx.transaction['fee_quantity'] = None
                buy_tx.transaction['fee_currency'] = None
                buy_tx.transaction['fee_gbp'] = None
                buy_tx.transaction['source_transaction_id'] = fill['order_id']
                buy_tx.transaction['source_trade_id'] = fill['trade_id']

                buy_df = pd.DataFrame(buy_tx.transaction, index=[0])

                return pd.concat([buy_df, sell_df])

            else:
                sell_tx = Transaction()

                sell_tx.transaction['asset'] = fill['product_id'].split('-')[1]
                sell_tx.transaction['action'] = 'exchange_crypto_for_crypto'
                sell_tx.transaction['disposal'] = True
                sell_tx.transaction['datetime'] = fill['created_at']
                sell_tx.transaction['initial_asset_quantity'] = float(fill['price']) * float(fill['size'])
                sell_tx.transaction['initial_asset_currency'] = fill['product_id'].split('-')[1]
                sell_tx.transaction['initial_asset_location'] = 'Coinbase Pro'
                sell_tx.transaction['initial_asset_address'] = None
                sell_tx.transaction['price'] = fill['price']
                sell_tx.transaction['final_asset_quantity'] = fill['size']
                sell_tx.transaction['final_asset_currency'] = fill['product_id'].split('-')[0]
                sell_tx.transaction['final_asset_gbp'] = None
                sell_tx.transaction['final_asset_location'] = 'Coinbase Pro'
                sell_tx.transaction['final_asset_address'] = None
                sell_tx.transaction['fee_type'] = 'exchange'
                sell_tx.transaction['fee_quantity'] = fill['fee']
                sell_tx.transaction['fee_currency'] = fill['product_id'].split('-')[1]
                sell_tx.transaction['fee_gbp'] = None
                sell_tx.transaction['source_transaction_id'] = fill['order_id']
                sell_tx.transaction['source_trade_id'] = fill['trade_id']

                sell_df = pd.DataFrame(sell_tx.transaction, index=[0])

                buy_tx = Transaction()

                buy_tx.transaction['asset'] = fill['product_id'].split('-')[0]
                buy_tx.transaction['action'] = 'exchange_crypto_for_crypto'
                buy_tx.transaction['disposal'] = False
                buy_tx.transaction['datetime'] = fill['created_at']
                buy_tx.transaction['initial_asset_quantity'] = float(fill['price']) * float(fill['size'])
                buy_tx.transaction['initial_asset_currency'] = fill['product_id'].split('-')[1]
                buy_tx.transaction['initial_asset_location'] = 'Coinbase Pro'
                buy_tx.transaction['initial_asset_address'] = None
                buy_tx.transaction['price'] = fill['price']
                buy_tx.transaction['final_asset_quantity'] = fill['size']
                buy_tx.transaction['final_asset_currency'] = fill['product_id'].split('-')[0]
                buy_tx.transaction['final_asset_gbp'] = None
                buy_tx.transaction['final_asset_location'] = 'Coinbase Pro'
                buy_tx.transaction['final_asset_address'] = None
                buy_tx.transaction['fee_type'] = 'exchange'
                buy_tx.transaction['fee_quantity'] = None
                buy_tx.transaction['fee_currency'] = None
                buy_tx.transaction['fee_gbp'] = None
                buy_tx.transaction['source_transaction_id'] = fill['order_id']
                buy_tx.transaction['source_trade_id'] = fill['trade_id']

                buy_df = pd.DataFrame(buy_tx.transaction, index=[0])

                return pd.concat([buy_df, sell_df])

    def get_deposits(self, accounts):
        """
        Method that retrieves all deposits for all assets
        """

        path = self.base_url + 'transfers'

        # Set up authentication and make call to the API
        params = {'type': 'deposit'}
        auth = CoinbaseProAuth(self.api_key, self.api_secret, self.passphrase)
        r = requests.get(path, auth=auth, params=params).json()

        # Lookup and attach the asset of the deposit
        for i in r:
            i['currency'] = accounts.get(i['account_id'])

        return r

    def get_withdrawals(self, accounts):
        """
        Method that retrieves all withdrawals for all assets
        """

        path = self.base_url + 'transfers'

        # Set up authentication and make call to the API
        params = {'type': 'withdraw'}
        auth = CoinbaseProAuth(self.api_key, self.api_secret, self.passphrase)
        r = requests.get(path, auth=auth, params=params).json()

        # Lookup and attach the asset of the withdrawal
        for i in r:
            i['currency'] = accounts.get(i['account_id'])

        return r


if __name__ == '__main__':
    x = CoinbasePro()
    x.get_coinbase_pro_transactions()
    # accounts = x.get_accounts()
    # x.get_deposits(accounts)
