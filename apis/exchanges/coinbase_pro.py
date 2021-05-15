import os
import requests
import pandas as pd
from datetime import datetime as dt

from apis.authentication import CoinbaseProAuth
from apis.helpers import Transaction, CoinbaseConvertToGBP


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

        # Get list of products
        products = self.get_products()

        # Loop through products and keep products with fills transactions
        all_fills = []
        for product_id in products:
            transactions = self.get_fills(product_id)
            if transactions:
                all_fills = all_fills + transactions

        # Create dataframe of all fills transactions
        all_fills_dataframes = []
        for fill in all_fills:
            all_fills_dataframes.append(CoinbasePro.create_fills_dataframe(fill))

        df_fills = pd.concat(all_fills_dataframes)

        # Get all account ids and their corresponding assets
        accounts = self.get_accounts()

        # Get all deposit transactions
        all_deposits = self.get_deposits(accounts)

        # Create dataframe of all deposit transactions
        all_deposits_dataframes = []
        for deposit in all_deposits:
            all_deposits_dataframes.append(CoinbasePro.create_deposits_dataframe(deposit))

        df_deposits = pd.concat(all_deposits_dataframes)

        # Get all withdrawal transactions
        all_withdrawals = self.get_withdrawals(accounts)

        # Create dataframe of all withdrawal transactions
        all_withdrawals_dataframe = []
        for withdrawal in all_withdrawals:
            all_withdrawals_dataframe.append(CoinbasePro.create_withdrawals_dataframe(withdrawal))

        df_withdrawals = pd.concat(all_withdrawals_dataframe)

        # Union all transaction dataframes and sort by datetime
        df_transactions = pd.concat([df_fills, df_deposits, df_withdrawals]).sort_values(by='datetime')

        # Loop through and get GBP values where missing
        final_asset_gbp = []
        fee_gbp = []
        for row in df_transactions.itertuples():
            # Calculate GBP value for all disposals
            if row.action in ['exchange_fiat_for_crypto', 'exchange_crypto_for_fiat', 'exchange_crypto_for_crypto']:
                if row.final_asset_currency == 'GBP':
                    final_asset_gbp.append(row.final_asset_quantity)
                else:
                    asset = row.final_asset_currency
                    datetime = dt.strptime(row.datetime, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d %H:%M:00')
                    quantity = row.final_asset_quantity
                    c = CoinbaseConvertToGBP(asset, datetime, quantity)
                    gbp_value = c.convert_to_gbp()
                    final_asset_gbp.append(gbp_value)
            else:
                final_asset_gbp.append(None)

            # Calculate all fees in GBP
            if not pd.isna(row.fee_currency):
                if any([row.fee_currency == 'GBP', row.fee_quantity == 0.0]):
                    fee_gbp.append(row.fee_quantity)
                else:
                    asset = row.fee_currency
                    datetime = dt.strptime(row.datetime, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d %H:%M:00')
                    quantity = row.fee_quantity
                    c = CoinbaseConvertToGBP(asset, datetime, quantity)
                    gbp_value = c.convert_to_gbp()
                    fee_gbp.append(gbp_value)
            else:
                fee_gbp.append(None)

        df_transactions['final_asset_gbp'] = final_asset_gbp
        df_transactions['fee_gbp'] = fee_gbp

        return df_transactions

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
        """
        Return dict of all account_ids and their corresponding currency
        """

        path = self.base_url + 'accounts'

        # Set up authentication and make call to the API
        auth = CoinbaseProAuth(self.api_key, self.api_secret, self.passphrase)
        r = requests.get(path, auth=auth).json()

        # Create dict of all currencies and their account ids
        accounts = {i['id']: i['currency'] for i in r}

        return accounts

    def get_fills(self, product_id):
        """
        Function to return fills transactions if there has ever been a transaction for a given product id
        """

        # Set up authentication and make call to the API
        path = self.base_url + 'fills'
        params = {'product_id': f'{product_id}'}
        auth = CoinbaseProAuth(self.api_key, self.api_secret, self.passphrase)
        r = requests.get(path, auth=auth, params=params).json()

        # Only return response if the product_id has any fills transactions
        if len(r) > 0:
            return r

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

    @staticmethod
    def create_fills_dataframe(fill):
        """
        Take individual fill transaction and use to populate a Transaction object
        """

        # Handle fiat transactions (buy/sell)
        if fill['product_id'].split('-')[1] in ['GBP', 'EUR']:
            side = fill['side']

            # Buying crypto with fiat
            if side == 'buy':
                tx = Transaction()

                tx.transaction['asset'] = fill['product_id'].split('-')[0]
                tx.transaction['action'] = 'exchange_fiat_for_crypto'
                tx.transaction['disposal'] = False
                tx.transaction['datetime'] = dt.strptime(fill['created_at'],
                                                         '%Y-%m-%dT%H:%M:%S.%fZ').strftime('%Y-%m-%d %H:%M:%S')
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

            # Selling crypto for fiat
            else:
                tx = Transaction()

                tx.transaction['asset'] = fill['product_id'].split('-')[0]
                tx.transaction['action'] = 'exchange_crypto_for_fiat'
                tx.transaction['disposal'] = True
                tx.transaction['datetime'] = dt.strptime(fill['created_at'],
                                                         '%Y-%m-%dT%H:%M:%S.%fZ').strftime('%Y-%m-%d %H:%M:%S')
                tx.transaction['initial_asset_quantity'] = float(fill['size'])
                tx.transaction['initial_asset_currency'] = fill['product_id'].split('-')[0]
                tx.transaction['initial_asset_location'] = 'Coinbase Pro'
                tx.transaction['initial_asset_address'] = None
                tx.transaction['price'] = fill['price']
                tx.transaction['final_asset_quantity'] = float(fill['price']) * float(fill['size'])
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

        # Exchanging crypto for crypto
        else:
            side = fill['side']

            if side == 'sell':
                sell_tx = Transaction()

                sell_tx.transaction['asset'] = fill['product_id'].split('-')[0]
                sell_tx.transaction['action'] = 'exchange_crypto_for_crypto'
                sell_tx.transaction['disposal'] = True
                sell_tx.transaction['datetime'] = dt.strptime(fill['created_at'],
                                                              '%Y-%m-%dT%H:%M:%S.%fZ').strftime('%Y-%m-%d %H:%M:%S')
                sell_tx.transaction['initial_asset_quantity'] = fill['size']
                sell_tx.transaction['initial_asset_currency'] = fill['product_id'].split('-')[0]
                sell_tx.transaction['initial_asset_location'] = 'Coinbase Pro'
                sell_tx.transaction['initial_asset_address'] = None
                sell_tx.transaction['price'] = fill['price']
                sell_tx.transaction['final_asset_quantity'] = float(fill['price']) * float(fill['size'])
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
                buy_tx.transaction['datetime'] = dt.strptime(fill['created_at'],
                                                             '%Y-%m-%dT%H:%M:%S.%fZ').strftime('%Y-%m-%d %H:%M:%S')
                buy_tx.transaction['initial_asset_quantity'] = fill['size']
                buy_tx.transaction['initial_asset_currency'] = fill['product_id'].split('-')[0]
                buy_tx.transaction['initial_asset_location'] = 'Coinbase Pro'
                buy_tx.transaction['initial_asset_address'] = None
                buy_tx.transaction['price'] = fill['price']
                buy_tx.transaction['final_asset_quantity'] = float(fill['price']) * float(fill['size'])
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
                sell_tx.transaction['datetime'] = dt.strptime(fill['created_at'],
                                                              '%Y-%m-%dT%H:%M:%S.%fZ').strftime('%Y-%m-%d %H:%M:%S')
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
                buy_tx.transaction['datetime'] = dt.strptime(fill['created_at'],
                                                             '%Y-%m-%dT%H:%M:%S.%fZ').strftime('%Y-%m-%d %H:%M:%S')
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

    @staticmethod
    def create_deposits_dataframe(deposit):
        """
        Take individual deposit transaction and use to populate a Transaction object
        """

        if deposit['completed_at']:  # Don't process if deposit was never completed
            tx = Transaction()

            tx.transaction['asset'] = deposit['currency']
            if deposit['currency'] in ['GBP', 'EUR']:
                tx.transaction['action'] = 'deposit_fiat'
            else:
                tx.transaction['action'] = 'deposit_crypto'
            if 'crypto_address' in deposit['details'].keys():
                tx.transaction['type'] = 'external'
            elif deposit['currency'] in ['GBP', 'EUR']:
                tx.transaction['type'] = None
            else:
                tx.transaction['type'] = 'deposit_from_coinbase'
            tx.transaction['disposal'] = False
            tx.transaction['datetime'] = dt.strptime(deposit['created_at'],
                                                     '%Y-%m-%d %H:%M:%S.%f+00').strftime('%Y-%m-%d %H:%M:%S')
            tx.transaction['initial_asset_quantity'] = deposit['amount']
            tx.transaction['initial_asset_currency'] = deposit['currency']
            if 'crypto_address' in deposit['details'].keys():
                tx.transaction['initial_asset_location'] = None
            else:
                if deposit['currency'] not in ['GBP', 'EUR']:
                    tx.transaction['initial_asset_location'] = 'Coinbase'
                else:
                    tx.transaction['initial_asset_location'] = None
            tx.transaction['initial_asset_address'] = deposit['details'].get('crypto_address')
            tx.transaction['price'] = None
            tx.transaction['final_asset_quantity'] = deposit['amount']
            tx.transaction['final_asset_currency'] = deposit['currency']
            tx.transaction['final_asset_gbp'] = None
            tx.transaction['final_asset_location'] = 'Coinbase Pro'
            tx.transaction['final_asset_address'] = None
            tx.transaction['fee_type'] = 'exchange'
            tx.transaction['fee_quantity'] = None
            tx.transaction['fee_currency'] = None
            tx.transaction['fee_gbp'] = None
            tx.transaction['source_transaction_id'] = deposit['id']
            tx.transaction['source_trade_id'] = None

            return pd.DataFrame(tx.transaction, index=[0])

    @staticmethod
    def create_withdrawals_dataframe(withdrawal):
        """
        Take individual withdrawal transaction and use to populate a Transaction object
        """

        if withdrawal['completed_at']:  # Don't process if deposit was never completed
            tx = Transaction()

            tx.transaction['asset'] = withdrawal['currency']
            if withdrawal['currency'] in ['GBP', 'EUR']:
                tx.transaction['action'] = 'withdraw_fiat'
            else:
                tx.transaction['action'] = 'withdraw_crypto'
            if any(['crypto_address' in withdrawal['details'].keys(),
                    'sent_to_address' in withdrawal['details'].keys()]):
                tx.transaction['type'] = 'external'
                tx.transaction['final_asset_location'] = None
                try:
                    tx.transaction['final_asset_address'] = withdrawal['details']['crypto_address']
                except:
                    tx.transaction['final_asset_address'] = withdrawal['details'].get('sent_to_address')
            elif withdrawal['currency'] in ['GBP', 'EUR']:
                tx.transaction['type'] = None
                tx.transaction['final_asset_address'] = None
                tx.transaction['final_asset_location'] = None
            else:
                tx.transaction['type'] = 'withdraw_to_coinbase'
                tx.transaction['final_asset_address'] = None
                tx.transaction['final_asset_location'] = 'Coinbase'
            tx.transaction['disposal'] = False
            tx.transaction['datetime'] = dt.strptime(withdrawal['created_at'],
                                                     '%Y-%m-%d %H:%M:%S.%f+00').strftime('%Y-%m-%d %H:%M:%S')
            tx.transaction['initial_asset_quantity'] = withdrawal['amount']
            tx.transaction['initial_asset_currency'] = withdrawal['currency']
            tx.transaction['initial_asset_location'] = 'Coinbase Pro'
            tx.transaction['initial_asset_address'] = None
            tx.transaction['price'] = None
            tx.transaction['final_asset_quantity'] = withdrawal['amount']
            tx.transaction['final_asset_currency'] = withdrawal['currency']
            tx.transaction['final_asset_gbp'] = None
            tx.transaction['fee_type'] = 'withdrawal'
            tx.transaction['fee_quantity'] = withdrawal['details'].get('fee', 0)
            tx.transaction['fee_currency'] = withdrawal['currency']
            tx.transaction['fee_gbp'] = None
            tx.transaction['source_transaction_id'] = withdrawal['id']
            tx.transaction['source_trade_id'] = None

            return pd.DataFrame(tx.transaction, index=[0])


if __name__ == '__main__':
    x = CoinbasePro()
    x.get_coinbase_pro_transactions()
    # accounts = x.get_accounts()
    # x.get_deposits(accounts)
