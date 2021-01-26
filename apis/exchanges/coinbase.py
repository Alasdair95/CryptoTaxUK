import os
import re
import requests
import pandas as pd

from apis.authentication import CoinbaseAuth
from apis.helpers import Transaction


class Coinbase:
    def __init__(self):
        self.api_key = os.environ.get('COINBASE_API_KEY')
        self.api_secret = os.environ.get('COINBASE_API_SECRET')
        self.base_url = 'https://api.coinbase.com/v2/'

    def get_coinbase_transactions(self):
        """
        Method to execute all other methods in correct order to return all historical transactions from Coinbase.
        """

        # Get list of all wallets Coinbase allows
        wallets = self.get_wallets()

        # Loop through all wallets and ping its transactions endpoint to see which assets have transactions
        wallets_with_transactions = []
        for wallet in wallets:
            if self.transactions_exist(wallet):
                wallets_with_transactions.append(wallet)

        # Loop through wallets we know have transactions and return a dict with all historical transactions
        all_transactions = []
        for wallet in wallets_with_transactions:
            all_transactions.append({wallet['asset']: self.get_transactions(wallet['id'])})

        # Convert all transactions into pandas dataframes
        all_transactions_dataframes = []
        for transactions in all_transactions:
            all_transactions_dataframes.append(Coinbase.create_transactions_dataframe(transactions))

        return pd.concat(all_transactions_dataframes)

    @staticmethod
    def pagination(response):
        """
        Determine whether or not the response has multiple pages
        """

        if response['pagination']['next_uri']:
            return response['pagination']['next_uri']

    def get_wallets(self):
        """
        Function to return a list of dicts containing information on each asset wallet.
        """

        # Set up authentication and make call to the API
        auth = CoinbaseAuth(self.api_key, self.api_secret)
        r = requests.get(self.base_url+'accounts', auth=auth).json()

        wallets = []
        for wallet in r['data']:
            if wallet['type'] == 'wallet':
                d = {
                    'id': wallet['id'],
                    'asset': wallet['currency']['code'],
                    'current_balance': wallet['balance']['amount']
                }
                wallets.append(d)

        return wallets

    def transactions_exist(self, wallet):
        """
        Method to determine whether a give asset wallet has any transactions
        """

        # Set up authentication and make call to the API
        auth = CoinbaseAuth(self.api_key, self.api_secret)
        r = requests.get(self.base_url + f"accounts/{wallet['id']}/transactions", auth=auth).json()

        # If there are transactions return True
        if len(r['data']) > 0:
            return True

    def get_transactions(self, account_id):
        """
        Return all transactions for a given account id
        """

        # Initialise empty list to hold all individual transactions
        transactions = []

        # Set up authentication and make call to the API
        auth = CoinbaseAuth(self.api_key, self.api_secret)
        r = requests.get(self.base_url+f'accounts/{account_id}/transactions', auth=auth).json()

        # Add first page of results to list
        for transaction in r['data']:
            transactions.append(transaction)

        # Check whether we need to call multiple pages of transactions
        pagination = Coinbase.pagination(r)

        while pagination:
            # Set up authentication and make call to the API
            auth = CoinbaseAuth(self.api_key, self.api_secret)
            r = requests.get(self.base_url+'/'.join(pagination.split('/')[2:]), auth=auth).json()

            # Add new page of results to list
            for transaction in r['data']:
                transactions.append(transaction)

            # Check for further pagination
            pagination = Coinbase.pagination(r)

        # Return full set of transactions
        return transactions

    @staticmethod
    def handle_trade_transaction(asset, transaction):
        tx = Transaction()

        re_converted_from = re.compile(r'^Converted from ')

        if re_converted_from.search(transaction['details']['title']):
            tx.transaction['initial_asset_quantity'] = abs(float(transaction['amount']['amount']))
            tx.transaction['initial_asset_currency'] = asset
            tx.transaction['final_asset_quantity'] = None
            tx.transaction['final_asset_currency'] = None
            tx.transaction['final_asset_gbp'] = None
        else:
            tx.transaction['initial_asset_quantity'] = None
            tx.transaction['initial_asset_currency'] = None
            tx.transaction['final_asset_quantity'] = abs(float(transaction['amount']['amount']))
            tx.transaction['final_asset_currency'] = asset
            tx.transaction['final_asset_gbp'] = abs(float(transaction['native_amount']['amount']))

        tx.transaction['asset'] = asset
        tx.transaction['action'] = 'exchange_crypto_for_crypto'
        tx.transaction['disposal'] = True
        tx.transaction['datetime'] = transaction['created_at']
        tx.transaction['initial_asset_location'] = 'Coinbase'
        tx.transaction['initial_asset_address'] = None
        tx.transaction['price'] = None
        tx.transaction['final_asset_location'] = 'Coinbase'
        tx.transaction['final_asset_address'] = None
        tx.transaction['fee_type'] = 'exchange'
        tx.transaction['fee_quantity'] = None
        tx.transaction['fee_currency'] = None
        tx.transaction['fee_gbp'] = None
        tx.transaction['source_transaction_id'] = transaction['id']
        tx.transaction['source_trade_id'] = transaction['trade']['id']

        return pd.DataFrame(tx.transaction, index=[0])

    @staticmethod
    def handle_buy_transaction(asset, transaction):
        tx = Transaction()

        tx.transaction['asset'] = asset
        tx.transaction['action'] = 'exchange_fiat_for_crypto'
        tx.transaction['disposal'] = False
        tx.transaction['datetime'] = transaction['created_at']
        tx.transaction['initial_asset_quantity'] = None
        tx.transaction['initial_asset_currency'] = transaction['details']['payment_method_name'].split(' ')[0]
        tx.transaction['initial_asset_location'] = 'Coinbase'
        tx.transaction['initial_asset_address'] = None
        tx.transaction['price'] = None
        tx.transaction['final_asset_quantity'] = abs(float(transaction['amount']['amount']))
        tx.transaction['final_asset_currency'] = asset
        tx.transaction['final_asset_gbp'] = abs(float(transaction['native_amount']['amount']))
        tx.transaction['final_asset_location'] = 'Coinbase'
        tx.transaction['final_asset_address'] = None
        tx.transaction['fee_type'] = 'exchange'
        tx.transaction['fee_quantity'] = None
        tx.transaction['fee_currency'] = None
        tx.transaction['fee_gbp'] = None
        tx.transaction['source_transaction_id'] = transaction['id']
        tx.transaction['source_trade_id'] = transaction['buy']['id']

        return pd.DataFrame(tx.transaction, index=[0])

    @staticmethod
    def handle_sell_transaction(asset, transaction):
        # Not had one here yet! But gonna take a punt based on the buy transaction response
        print(f'Sell transaction: {transaction}')
        tx = Transaction()

        tx.transaction['asset'] = asset
        tx.transaction['action'] = 'exchange_fiat_for_crypto'
        tx.transaction['disposal'] = False
        tx.transaction['datetime'] = transaction['created_at']
        tx.transaction['initial_asset_quantity'] = None
        tx.transaction['initial_asset_currency'] = transaction['details']['payment_method_name'].split(' ')[0]
        tx.transaction['initial_asset_location'] = 'Coinbase'
        tx.transaction['initial_asset_address'] = None
        tx.transaction['price'] = None
        tx.transaction['final_asset_quantity'] = abs(float(transaction['amount']['amount']))
        tx.transaction['final_asset_currency'] = transaction['details']['payment_method_name'].split(' ')[0]
        tx.transaction['final_asset_gbp'] = abs(float(transaction['native_amount']['amount']))
        tx.transaction['final_asset_location'] = 'Coinbase'
        tx.transaction['final_asset_address'] = None
        tx.transaction['fee_type'] = 'exchange'
        tx.transaction['fee_quantity'] = None
        tx.transaction['fee_currency'] = None
        tx.transaction['fee_gbp'] = None
        tx.transaction['source_transaction_id'] = transaction['id']
        tx.transaction['source_trade_id'] = transaction['buy']['id']

        return pd.DataFrame(tx.transaction, index=[0])

    @staticmethod
    def handle_send_transaction(asset, transaction):
        tx = Transaction()

        re_sent = re.compile(r'^Sent ')
        re_received = re.compile(r'^Received ')

        if transaction['details']['subtitle'] == 'From Coinbase Earn':
            tx.transaction['action'] = 'gifted_crypto'
            tx.transaction['initial_asset_location'] = None
            tx.transaction['final_asset_address'] = None
            tx.transaction['source_trade_id'] = transaction['from']['id']
            tx.transaction['initial_asset_quantity'] = None
            tx.transaction['initial_asset_currency'] = None
            tx.transaction['final_asset_quantity'] = abs(float(transaction['amount']['amount']))
            tx.transaction['final_asset_currency'] = asset
            tx.transaction['final_asset_gbp'] = abs(float(transaction['native_amount']['amount']))
            tx.transaction['fee_type'] = None
            tx.transaction['fee_quantity'] = None
            tx.transaction['fee_currency'] = None
            tx.transaction['fee_gbp'] = None
        elif re_sent.search(transaction['details']['title']):
            tx.transaction['action'] = 'withdraw_crypto'
            tx.transaction['initial_asset_location'] = 'Coinbase'
            tx.transaction['final_asset_address'] = transaction['to']['address']
            if 'application' in transaction.keys():
                tx.transaction['source_trade_id'] = transaction['application']['id']
            else:
                tx.transaction['source_transaction_id'] = None
            tx.transaction['initial_asset_quantity'] = abs(float(transaction['amount']['amount']))
            tx.transaction['initial_asset_currency'] = asset
            tx.transaction['final_asset_quantity'] = None
            tx.transaction['final_asset_currency'] = None
            tx.transaction['final_asset_gbp'] = None
            tx.transaction['fee_type'] = 'transfer'
            if 'transaction_fee' in transaction.keys():
                tx.transaction['fee_quantity'] = abs(float(transaction['transaction_fee']['amount']))
                tx.transaction['fee_currency'] = transaction['transaction_fee']['currency']
            else:
                tx.transaction['fee_quantity'] = abs(float(transaction['network']['transaction_fee']['amount']))
                tx.transaction['fee_currency'] = transaction['network']['transaction_fee']['currency']
            tx.transaction['fee_gbp'] = None
        elif re_received.search(transaction['details']['title']):
            tx.transaction['action'] = 'deposit_crypto'
            tx.transaction['initial_asset_location'] = None
            tx.transaction['final_asset_address'] = None
            tx.transaction['source_trade_id'] = None
            tx.transaction['initial_asset_quantity'] = None
            tx.transaction['initial_asset_currency'] = None
            tx.transaction['final_asset_quantity'] = abs(float(transaction['amount']['amount']))
            tx.transaction['final_asset_currency'] = asset
            tx.transaction['final_asset_gbp'] = abs(float(transaction['native_amount']['amount']))
            tx.transaction['fee_type'] = None
            tx.transaction['fee_quantity'] = None
            tx.transaction['fee_currency'] = None
            tx.transaction['fee_gbp'] = None
        else:
            print(f"New send action: {transaction['details']['title']}")

        tx.transaction['asset'] = asset
        tx.transaction['disposal'] = False
        tx.transaction['datetime'] = transaction['created_at']
        tx.transaction['initial_asset_address'] = None
        tx.transaction['price'] = None
        tx.transaction['final_asset_location'] = 'Coinbase'
        tx.transaction['source_transaction_id'] = transaction['id']

        return pd.DataFrame(tx.transaction, index=[0])

    @staticmethod
    def handle_exchange_deposit_transaction(asset, transaction):
        tx = Transaction()

        tx.transaction['asset'] = asset
        tx.transaction['action'] = 'withdraw_crypto'
        tx.transaction['disposal'] = False
        tx.transaction['datetime'] = transaction['created_at']
        tx.transaction['initial_asset_quantity'] = abs(float(transaction['amount']['amount']))
        tx.transaction['initial_asset_currency'] = asset
        tx.transaction['initial_asset_location'] = 'Coinbase'
        tx.transaction['initial_asset_address'] = None
        tx.transaction['price'] = None
        tx.transaction['final_asset_quantity'] = abs(float(transaction['amount']['amount']))
        tx.transaction['final_asset_currency'] = asset
        tx.transaction['final_asset_gbp'] = abs(float(transaction['native_amount']['amount']))
        tx.transaction['final_asset_location'] = 'Coinbase Pro'
        tx.transaction['final_asset_address'] = None
        tx.transaction['fee_type'] = 'exchange'
        tx.transaction['fee_quantity'] = None
        tx.transaction['fee_currency'] = None
        tx.transaction['fee_gbp'] = None
        tx.transaction['source_transaction_id'] = transaction['id']
        if 'application' in transaction.keys():
            tx.transaction['source_trade_id'] = transaction['application']['id']
        else:
            tx.transaction['source_transaction_id'] = None

        return pd.DataFrame(tx.transaction, index=[0])

    @staticmethod
    def handle_exchange_withdrawal_transaction(asset, transaction):
        # Has an accompanying 'send' transaction that covers it
        pass

    @staticmethod
    def handle_pro_deposit_transaction(asset, transaction):
        tx = Transaction()

        tx.transaction['asset'] = asset
        tx.transaction['action'] = 'withdraw_crypto'
        tx.transaction['disposal'] = False
        tx.transaction['datetime'] = transaction['created_at']
        tx.transaction['initial_asset_quantity'] = abs(float(transaction['amount']['amount']))
        tx.transaction['initial_asset_currency'] = asset
        tx.transaction['initial_asset_location'] = 'Coinbase'
        tx.transaction['initial_asset_address'] = None
        tx.transaction['price'] = None
        tx.transaction['final_asset_quantity'] = abs(float(transaction['amount']['amount']))
        tx.transaction['final_asset_currency'] = asset
        tx.transaction['final_asset_gbp'] = abs(float(transaction['native_amount']['amount']))
        tx.transaction['final_asset_location'] = 'Coinbase Pro'
        tx.transaction['final_asset_address'] = None
        tx.transaction['fee_type'] = 'exchange'
        tx.transaction['fee_quantity'] = None
        tx.transaction['fee_currency'] = None
        tx.transaction['fee_gbp'] = None
        tx.transaction['source_transaction_id'] = transaction['id']
        tx.transaction['source_trade_id'] = transaction['application']['id']

        return pd.DataFrame(tx.transaction, index=[0])

    @staticmethod
    def handle_pro_withdrawal_transaction(asset, transaction):
        # Has an accompanying 'send' transaction that covers it
        pass

    @staticmethod
    def create_transactions_dataframe(transaction_history):
        """
        Convert the raw transaction data to a pandas dataframe containing cleaned data
        """

        transaction_type_function_map = {
            'trade': Coinbase.handle_trade_transaction,
            'buy': Coinbase.handle_buy_transaction,
            'sell': Coinbase.handle_sell_transaction,
            'send': Coinbase.handle_send_transaction,
            'exchange_deposit': Coinbase.handle_exchange_deposit_transaction,
            'exchange_withdrawal': Coinbase.handle_exchange_withdrawal_transaction,
            'pro_deposit': Coinbase.handle_pro_deposit_transaction,
            'pro_withdrawal': Coinbase.handle_pro_withdrawal_transaction
        }

        transaction_dataframes = []
        for k, v in transaction_history.items():
            for i in v:
                transaction_dataframes.append(transaction_type_function_map.get(i['type'])(k, i))

        transaction_df = pd.concat(transaction_dataframes)

        transaction_df.sort_values(by='datetime', inplace=True)

        transaction_df.reset_index(drop=True, inplace=True)

        return transaction_df

    def test(self):
        # Set up authentication and make call to the API
        path = self.base_url + 'accounts/19e01845-35e2-5553-91f3-f48050a0c09a/trades/896bc724-f386-57f2-a401-4ebfb6350fbe'
        auth = CoinbaseAuth(self.api_key, self.api_secret)
        r = requests.get(path, auth=auth).json()
        print(r)


if __name__ == '__main__':
    x = Coinbase()
    x.get_coinbase_transactions()
    # x.get_transactions('f7b61c43-93c9-5b94-b4b0-719e09ad9888')
    t = {'BAND': [{'id': '65bf3819-a9cd-58c5-9e89-2d519ba052f9', 'type': 'trade', 'status': 'completed', 'amount': {'amount': '-0.49046370', 'currency': 'BAND'}, 'native_amount': {'amount': '-2.23', 'currency': 'GBP'}, 'description': None, 'created_at': '2020-12-22T13:30:16Z', 'updated_at': '2020-12-22T13:30:16Z', 'resource': 'transaction', 'resource_path': '/v2/accounts/b06cf0ff-981a-5887-bdf8-8436f58c5e71/transactions/65bf3819-a9cd-58c5-9e89-2d519ba052f9', 'instant_exchange': False, 'trade': {'id': '896bc724-f386-57f2-a401-4ebfb6350fbe', 'resource': 'trade', 'resource_path': '/v2/accounts/19e01845-35e2-5553-91f3-f48050a0c09a/trades/896bc724-f386-57f2-a401-4ebfb6350fbe'}, 'details': {'title': 'Converted from Band Protocol', 'subtitle': 'Using BAND Wallet', 'header': 'Converted 0.4904637 BAND (£2.23)', 'health': 'positive', 'payment_method_name': 'BAND Wallet'}}, {'id': 'aeb4976e-134b-5146-85a2-e0a797c40922', 'type': 'send', 'status': 'completed', 'amount': {'amount': '0.16360186', 'currency': 'BAND'}, 'native_amount': {'amount': '0.75', 'currency': 'GBP'}, 'description': None, 'created_at': '2020-12-22T13:29:01Z', 'updated_at': '2020-12-22T13:29:01Z', 'resource': 'transaction', 'resource_path': '/v2/accounts/b06cf0ff-981a-5887-bdf8-8436f58c5e71/transactions/aeb4976e-134b-5146-85a2-e0a797c40922', 'instant_exchange': False, 'off_chain_status': 'completed', 'network': {'status': 'off_blockchain', 'status_description': None}, 'from': {'id': '8a456d7f-fdca-5dd0-9e32-89f84ac433f0', 'resource': 'user', 'resource_path': '/v2/users/8a456d7f-fdca-5dd0-9e32-89f84ac433f0', 'currency': 'BAND'}, 'details': {'title': 'Received Band Protocol', 'subtitle': 'From Coinbase Earn', 'header': 'Received 0.16360186 BAND (£0.75)', 'health': 'positive'}}, {'id': '08c3ca12-2af7-5a8e-8c76-ac3f97f20c06', 'type': 'send', 'status': 'completed', 'amount': {'amount': '0.16360186', 'currency': 'BAND'}, 'native_amount': {'amount': '0.75', 'currency': 'GBP'}, 'description': None, 'created_at': '2020-12-22T13:28:27Z', 'updated_at': '2020-12-22T13:28:27Z', 'resource': 'transaction', 'resource_path': '/v2/accounts/b06cf0ff-981a-5887-bdf8-8436f58c5e71/transactions/08c3ca12-2af7-5a8e-8c76-ac3f97f20c06', 'instant_exchange': False, 'off_chain_status': 'completed', 'network': {'status': 'off_blockchain', 'status_description': None}, 'from': {'id': '8a456d7f-fdca-5dd0-9e32-89f84ac433f0', 'resource': 'user', 'resource_path': '/v2/users/8a456d7f-fdca-5dd0-9e32-89f84ac433f0', 'currency': 'BAND'}, 'details': {'title': 'Received Band Protocol', 'subtitle': 'From Coinbase Earn', 'header': 'Received 0.16360186 BAND (£0.75)', 'health': 'positive'}}, {'id': '72b806d3-d0b7-5c07-b4fc-5548d582f289', 'type': 'send', 'status': 'completed', 'amount': {'amount': '0.16325998', 'currency': 'BAND'}, 'native_amount': {'amount': '0.75', 'currency': 'GBP'}, 'description': None, 'created_at': '2020-12-22T13:28:00Z', 'updated_at': '2020-12-22T13:28:00Z', 'resource': 'transaction', 'resource_path': '/v2/accounts/b06cf0ff-981a-5887-bdf8-8436f58c5e71/transactions/72b806d3-d0b7-5c07-b4fc-5548d582f289', 'instant_exchange': False, 'off_chain_status': 'completed', 'network': {'status': 'off_blockchain', 'status_description': None}, 'from': {'id': '8a456d7f-fdca-5dd0-9e32-89f84ac433f0', 'resource': 'user', 'resource_path': '/v2/users/8a456d7f-fdca-5dd0-9e32-89f84ac433f0', 'currency': 'BAND'}, 'details': {'title': 'Received Band Protocol', 'subtitle': 'From Coinbase Earn', 'header': 'Received 0.16325998 BAND (£0.75)', 'health': 'positive'}}]}
    x.create_transactions_dataframe(t)
    # x.test()
