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
        all_transactions = []
        for product_id in products:
            transactions = self.transactions_exist(product_id)
            if transactions:
                all_transactions.append({product_id: transactions})

        # Loop through

        # Convert all transactions into pandas dataframes
        all_transactions_dataframes = []
        for transactions in all_transactions:
            all_transactions_dataframes.append(CoinbasePro.create_transactions_dataframe(transactions))

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
    def create_transactions_dataframe(transactions):
        pass

    def test(self):
            # Set up authentication and make call to the API
            path = self.base_url + 'fills'#/d7f4b258-23f4-4dd0-b920-daf2be191b79'
            params = {'product_id': 'ETH'}
            # params = {'type': 'withdraw'}
            auth = CoinbaseProAuth(self.api_key, self.api_secret, self.passphrase)
            # r = requests.get(path, auth=auth).json()
            r = requests.get(path, auth=auth, params=params).json()
            print(r)


if __name__ == '__main__':
    x = CoinbasePro()
    x.get_coinbase_pro_transactions()
    x.test()
