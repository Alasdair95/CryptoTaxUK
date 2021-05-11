import os
import re
import requests
import pandas as pd
from datetime import datetime as dt

from apis.authentication import CoinbaseAuth
from apis.helpers import Transaction, CoinbaseConvertToGBP


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

        # Loop through all wallets and ping its buys endpoint to see which assets have buys
        wallets_with_buys = []
        for wallet in wallets:
            if self.buys_exist(wallet):
                wallets_with_buys.append(wallet)

        # Loop through wallets we know have buys and return a dict with all historical buys
        all_buys = []
        for wallet in wallets_with_buys:
            all_buys.append({wallet['asset']: self.get_buys(wallet['id'])})

        # Convert all buys into pandas dataframes
        all_buys_dataframes = []
        for buys in all_buys:
            all_buys_dataframes.append(Coinbase.create_buys_dataframe(buys))

        if len(all_buys_dataframes) > 0:
            df_buys = pd.concat(all_buys_dataframes)
        else:
            df_buys = None

        # Loop through all wallets and ping its sells endpoint to see which assets have sells
        wallets_with_sells = []
        for wallet in wallets:
            if self.sells_exist(wallet):
                wallets_with_sells.append(wallet)

        # Loop through wallets we know have sells and return a dict with all historical sells
        all_sells = []
        for wallet in wallets_with_sells:
            all_sells.append({wallet['asset']: self.get_sells(wallet['id'])})

        # Convert all sells into pandas dataframes
        all_sells_dataframes = []
        for sells in all_sells:
            all_sells_dataframes.append(Coinbase.create_sells_dataframe(sells))

        if len(all_sells_dataframes) > 0:
            df_sells = pd.concat(all_sells_dataframes)
        else:
            df_sells = None

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

        if len(all_transactions_dataframes):
            df_transactions = pd.concat(all_transactions_dataframes)
        else:
            df_transactions = None

        df_all = pd.concat([df_buys, df_sells, df_transactions]).sort_values(by='datetime')

        # Remove deposits that are actually done in coinbase pro and are covered in coinbase_pro.py
        df_dep_1 = df_all.loc[((df_all['action'] == 'deposit_fiat') &
                               (df_all['type'] == 'fiat_deposit')) |
                              ((df_all['action'] == 'withdraw_fiat') &
                               (df_all['type'] == 'fiat_withdrawal')) |
                              ((df_all['action'] == 'withdraw_crypto') &
                               (df_all['type'] == 'send'))]

        df_dep_2 = df_all.loc[((df_all['action'] == 'withdraw_fiat') &
                               ((df_all['type'] == 'pro_deposit') |
                                (df_all['type'] == 'pro_withdrawal') |
                                (df_all['type'] == 'exchange_withdrawal'))) |
                              ((df_all['action'] == 'deposit_fiat') &
                               (df_all['type'] == 'exchange_deposit')) |
                              ((df_all['action'] == 'withdraw_crypto') &
                               ((df_all['type'] == 'exchange_withdrawal') |
                                (df_all['type'] == 'pro_withdrawal')))]

        deposits_to_remove = pd.merge(df_dep_1, df_dep_2, how='inner', left_on=['asset', 'final_asset_quantity'],
                                      right_on=['asset', 'final_asset_quantity'])

        deposit_source_transaction_ids_to_remove = deposits_to_remove['source_transaction_id_x'].tolist() + deposits_to_remove['source_transaction_id_y'].tolist()

        df_all = df_all[~df_all.source_transaction_id.isin(deposit_source_transaction_ids_to_remove)]

        # Remove withdrawals that are actually done in coinbase pro and are covered in coinbase_pro.py
        df_dep_1 = df_all.loc[((df_all['action'] == 'withdraw_crypto') &
                               (df_all['type'] == 'send'))]

        df_dep_2 = df_all.loc[((df_all['action'] == 'withdraw_crypto') &
                               ((df_all['type'] == 'exchange_withdrawal') |
                                (df_all['type'] == 'pro_withdrawal')))]

        withdrawals_to_remove = pd.merge(df_dep_1, df_dep_2, how='inner', left_on=['asset', 'initial_asset_quantity'],
                                         right_on=['asset', 'initial_asset_quantity'])

        withdrawal_source_transaction_ids_to_remove = withdrawals_to_remove['source_transaction_id_x'].tolist() + \
                                                      withdrawals_to_remove['source_transaction_id_y'].tolist()

        df_final = df_all[~df_all.source_transaction_id.isin(withdrawal_source_transaction_ids_to_remove)]

        # df_final = pd.read_csv(r"C:\Users\alasd\Documents\Projects Misc\coinbase.csv")

        df_crypto_crypto_buys = df_final.loc[(df_final['action'] == 'exchange_crypto_for_crypto') &
                                             pd.isna(df_final['initial_asset_currency'])].drop(columns=['initial_asset_quantity', 'initial_asset_currency'])

        df_crypto_crypto_sells = df_final.loc[(df_final['action'] == 'exchange_crypto_for_crypto') &
                                              pd.isna(df_final['final_asset_currency'])].drop(columns=['final_asset_quantity', 'final_asset_currency', 'final_asset_gbp'])

        df_cc_sells = pd.merge(df_crypto_crypto_sells, df_crypto_crypto_buys[['final_asset_quantity', 'final_asset_currency', 'final_asset_gbp', 'source_trade_id']], how='inner', left_on='source_trade_id', right_on='source_trade_id')

        df_cc_buys = pd.merge(df_crypto_crypto_buys, df_crypto_crypto_sells[['initial_asset_quantity', 'initial_asset_currency', 'source_trade_id']], how='inner', left_on='source_trade_id', right_on='source_trade_id')

        # Drop the origin 'exchange_crypto_for_crypto' records
        df_final = df_final.loc[df_final['action'] != 'exchange_crypto_for_crypto']

        df_final = df_final.append([df_cc_buys, df_cc_sells], sort=False)

        # Sort by datetime again
        df_final.sort_values(by='datetime', inplace=True)

        # Loop through and get GBP values where missing
        final_asset_gbp = []
        fee_gbp = []
        for row in df_final.itertuples():
            # Calculate GBP value for all disposals
            if row.disposal:
                if pd.isna(row.final_asset_gbp):
                    if not pd.isna(row.final_asset_gbp):
                        final_asset_gbp.append(None)
                    elif row.final_asset_currency == 'GBP':
                        final_asset_gbp.append(row.final_asset_quantity)
                    else:
                        asset = row.final_asset_currency
                        datetime = row.datetime.strftime('%Y-%m-%d %H:%M:00')
                        quantity = row.final_asset_quantity
                        c = CoinbaseConvertToGBP(asset, datetime, quantity)
                        gbp_value = c.convert_to_gbp()
                        final_asset_gbp.append(gbp_value)
                else:
                    final_asset_gbp.append(row.final_asset_gbp)
            else:
                final_asset_gbp.append(row.final_asset_gbp)

            # Calculate all fees in GBP
            if not pd.isna(row.fee_currency):
                if any([row.fee_currency == 'GBP', row.fee_quantity == 0.0]):
                    fee_gbp.append(row.fee_quantity)
                else:
                    asset = row.fee_currency
                    datetime = row.datetime.strftime('%Y-%m-%d %H:%M:00')
                    quantity = row.fee_quantity
                    c = CoinbaseConvertToGBP(asset, datetime, quantity)
                    gbp_value = c.convert_to_gbp()
                    fee_gbp.append(gbp_value)
            else:
                fee_gbp.append(None)

        df_final['final_asset_gbp'] = final_asset_gbp
        df_final['fee_gbp'] = fee_gbp

        return df_final

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
        r = requests.get(self.base_url + 'accounts', auth=auth).json()

        wallets = []
        for wallet in r['data']:
            # if wallet['type'] == 'wallet':  # Uncomment to remove fiat wallets
            d = {
                'id': wallet['id'],
                'asset': wallet['currency']['code'],
                'current_balance': wallet['balance']['amount']
            }
            wallets.append(d)

        pagination = Coinbase.pagination(r)

        while pagination:
            # Set up authentication and make call to the API
            auth = CoinbaseAuth(self.api_key, self.api_secret)
            r = requests.get(self.base_url + '/'.join(pagination.split('/')[2:]), auth=auth).json()

            for wallet in r['data']:
                # if wallet['type'] == 'wallet':  # Uncomment to remove fiat wallets
                d = {
                    'id': wallet['id'],
                    'asset': wallet['currency']['code'],
                    'current_balance': wallet['balance']['amount']
                }
                wallets.append(d)

            # Check for further pagination
            pagination = Coinbase.pagination(r)

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

    def buys_exist(self, wallet):
        """
        Method to determine whether a give asset wallet has any buys
        """

        # Set up authentication and make call to the API
        auth = CoinbaseAuth(self.api_key, self.api_secret)
        r = requests.get(self.base_url + f"accounts/{wallet['id']}/buys", auth=auth).json()

        # If there are transactions return True
        if len(r['data']) > 0:
            return True

    def sells_exist(self, wallet):
        """
        Method to determine whether a give asset wallet has any sells
        """

        # Set up authentication and make call to the API
        auth = CoinbaseAuth(self.api_key, self.api_secret)
        r = requests.get(self.base_url + f"accounts/{wallet['id']}/sells", auth=auth).json()

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
        r = requests.get(self.base_url + f'accounts/{account_id}/transactions', auth=auth).json()

        # Add first page of results to list
        for transaction in r['data']:
            transactions.append(transaction)

        # Check whether we need to call multiple pages of transactions
        pagination = Coinbase.pagination(r)

        while pagination:
            # Set up authentication and make call to the API
            auth = CoinbaseAuth(self.api_key, self.api_secret)
            r = requests.get(self.base_url + '/'.join(pagination.split('/')[2:]), auth=auth).json()

            # Add new page of results to list
            for transaction in r['data']:
                transactions.append(transaction)

            # Check for further pagination
            pagination = Coinbase.pagination(r)

        # Return full set of transactions
        return transactions

    def get_buys(self, account_id):
        """
        Return all buys for a given account id
        """

        # Initialise empty list to hold all individual transactions
        buys = []

        # Set up authentication and make call to the API
        auth = CoinbaseAuth(self.api_key, self.api_secret)
        r = requests.get(self.base_url + f'accounts/{account_id}/buys', auth=auth).json()

        # Add first page of results to list
        for buy in r['data']:
            buys.append(buy)

        # Check whether we need to call multiple pages of transactions
        pagination = Coinbase.pagination(r)

        while pagination:
            # Set up authentication and make call to the API
            auth = CoinbaseAuth(self.api_key, self.api_secret)
            r = requests.get(self.base_url + '/'.join(pagination.split('/')[2:]), auth=auth).json()

            # Add new page of results to list
            for buy in r['data']:
                buys.append(buy)

            # Check for further pagination
            pagination = Coinbase.pagination(r)

        # Return full set of transactions
        return buys

    def get_sells(self, account_id):
        """
        Return all sells for a given account id
        """

        # Initialise empty list to hold all individual transactions
        sells = []

        # Set up authentication and make call to the API
        auth = CoinbaseAuth(self.api_key, self.api_secret)
        r = requests.get(self.base_url + f'accounts/{account_id}/sells', auth=auth).json()

        # Add first page of results to list
        for sell in r['data']:
            sells.append(sell)

        # Check whether we need to call multiple pages of transactions
        pagination = Coinbase.pagination(r)

        while pagination:
            # Set up authentication and make call to the API
            auth = CoinbaseAuth(self.api_key, self.api_secret)
            r = requests.get(self.base_url + '/'.join(pagination.split('/')[2:]), auth=auth).json()

            # Add new page of results to list
            for sell in r['data']:
                sells.append(sell)

            # Check for further pagination
            pagination = Coinbase.pagination(r)

        # Return full set of transactions
        return sells

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
        tx.transaction['type'] = transaction['type']
        tx.transaction['action'] = 'exchange_crypto_for_crypto'
        tx.transaction['disposal'] = True
        tx.transaction['datetime'] = dt.strptime(transaction['created_at'], '%Y-%m-%dT%H:%M:%SZ')
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
        pass
        # tx = Transaction()
        #
        # tx.transaction['asset'] = asset
        # tx.transaction['type'] = transaction['type']
        # tx.transaction['action'] = 'exchange_fiat_for_crypto'
        # tx.transaction['disposal'] = False
        # tx.transaction['datetime'] = datetime.strptime(transaction['created_at'], '%Y-%m-%dT%H:%M:%SZ')
        # tx.transaction['initial_asset_quantity'] = None
        # tx.transaction['initial_asset_currency'] = transaction['details']['payment_method_name'].split(' ')[0]
        # tx.transaction['initial_asset_location'] = 'Coinbase'
        # tx.transaction['initial_asset_address'] = None
        # tx.transaction['price'] = None
        # tx.transaction['final_asset_quantity'] = abs(float(transaction['amount']['amount']))
        # tx.transaction['final_asset_currency'] = asset
        # tx.transaction['final_asset_gbp'] = abs(float(transaction['native_amount']['amount']))
        # tx.transaction['final_asset_location'] = 'Coinbase'
        # tx.transaction['final_asset_address'] = None
        # tx.transaction['fee_type'] = 'exchange'
        # tx.transaction['fee_quantity'] = None
        # tx.transaction['fee_currency'] = None
        # tx.transaction['fee_gbp'] = None
        # tx.transaction['source_transaction_id'] = transaction['id']
        # tx.transaction['source_trade_id'] = transaction['buy']['id']
        #
        # return pd.DataFrame(tx.transaction, index=[0])

    @staticmethod
    def handle_sell_transaction(asset, transaction):
        pass
        # Not had one here yet! But gonna take a punt based on the buy transaction response
        # print(f'Sell transaction: {transaction}')
        # tx = Transaction()
        #
        # tx.transaction['asset'] = asset
        # tx.transaction['type'] = transaction['type']
        # tx.transaction['action'] = 'exchange_crypto_for_fiat'
        # tx.transaction['disposal'] = False
        # tx.transaction['datetime'] = datetime.strptime(transaction['created_at'], '%Y-%m-%dT%H:%M:%SZ')
        # tx.transaction['initial_asset_quantity'] = None
        # tx.transaction['initial_asset_currency'] = transaction['details']['payment_method_name'].split(' ')[0]
        # tx.transaction['initial_asset_location'] = 'Coinbase'
        # tx.transaction['initial_asset_address'] = None
        # tx.transaction['price'] = None
        # tx.transaction['final_asset_quantity'] = abs(float(transaction['amount']['amount']))
        # tx.transaction['final_asset_currency'] = transaction['details']['payment_method_name'].split(' ')[0]
        # tx.transaction['final_asset_gbp'] = abs(float(transaction['native_amount']['amount']))
        # tx.transaction['final_asset_location'] = 'Coinbase'
        # tx.transaction['final_asset_address'] = None
        # tx.transaction['fee_type'] = 'exchange'
        # tx.transaction['fee_quantity'] = None
        # tx.transaction['fee_currency'] = None
        # tx.transaction['fee_gbp'] = None
        # tx.transaction['source_transaction_id'] = transaction['id']
        # tx.transaction['source_trade_id'] = transaction['buy']['id']
        #
        # return pd.DataFrame(tx.transaction, index=[0])

    @staticmethod
    def handle_send_transaction(asset, transaction):
        tx = Transaction()

        re_sent = re.compile(r'^Sent ')
        re_received = re.compile(r'^Received ')

        if transaction['details']['subtitle'] == 'From Coinbase Earn':
            tx.transaction['type'] = transaction['type']
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
            tx.transaction['type'] = transaction['type']
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
            tx.transaction['type'] = transaction['type']
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
        tx.transaction['datetime'] = dt.strptime(transaction['created_at'], '%Y-%m-%dT%H:%M:%SZ')
        tx.transaction['initial_asset_address'] = None
        tx.transaction['price'] = None
        tx.transaction['final_asset_location'] = 'Coinbase'
        tx.transaction['source_transaction_id'] = transaction['id']

        return pd.DataFrame(tx.transaction, index=[0])

    @staticmethod
    def handle_exchange_deposit_transaction(asset, transaction):
        tx = Transaction()

        tx.transaction['asset'] = asset
        tx.transaction['type'] = transaction['type']
        if asset in ['GBP', 'EUR']:
            tx.transaction['action'] = 'deposit_fiat'
        else:
            tx.transaction['action'] = 'deposit_crypto'
        tx.transaction['disposal'] = False
        tx.transaction['datetime'] = dt.strptime(transaction['created_at'], '%Y-%m-%dT%H:%M:%SZ')
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
        tx = Transaction()

        tx.transaction['asset'] = asset
        tx.transaction['type'] = transaction['type']
        if asset in ['GBP', 'EUR']:
            tx.transaction['action'] = 'withdraw_fiat'
        else:
            tx.transaction['action'] = 'withdraw_crypto'
        tx.transaction['disposal'] = False
        tx.transaction['datetime'] = dt.strptime(transaction['created_at'], '%Y-%m-%dT%H:%M:%SZ')
        tx.transaction['initial_asset_quantity'] = abs(float(transaction['amount']['amount']))
        tx.transaction['initial_asset_currency'] = asset
        if transaction['details']['subtitle'] == 'From Coinbase Pro':
            tx.transaction['initial_asset_location'] = 'Coinbase Pro'
        else:
            tx.transaction['initial_asset_location'] = 'Coinbase'
        tx.transaction['initial_asset_address'] = None
        tx.transaction['price'] = None
        tx.transaction['final_asset_quantity'] = abs(float(transaction['amount']['amount']))
        tx.transaction['final_asset_currency'] = asset
        tx.transaction['final_asset_gbp'] = None
        tx.transaction['final_asset_location'] = None
        tx.transaction['final_asset_address'] = None
        tx.transaction['fee_type'] = 'transfer'
        tx.transaction['fee_quantity'] = None
        tx.transaction['fee_currency'] = None
        tx.transaction['fee_gbp'] = None
        tx.transaction['source_transaction_id'] = transaction['id']
        tx.transaction['source_trade_id'] = None

        return pd.DataFrame(tx.transaction, index=[0])

    @staticmethod
    def handle_pro_deposit_transaction(asset, transaction):
        tx = Transaction()

        tx.transaction['asset'] = asset
        tx.transaction['type'] = transaction['type']
        if asset in ['GBP', 'EUR']:
            tx.transaction['action'] = 'withdraw_fiat'
        else:
            tx.transaction['action'] = 'withdraw_crypto'
        tx.transaction['disposal'] = False
        tx.transaction['datetime'] = dt.strptime(transaction['created_at'], '%Y-%m-%dT%H:%M:%SZ')
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
        tx = Transaction()

        tx.transaction['asset'] = asset
        tx.transaction['type'] = transaction['type']
        if asset in ['GBP', 'EUR']:
            tx.transaction['action'] = 'withdraw_fiat'
        else:
            tx.transaction['action'] = 'withdraw_crypto'
        tx.transaction['disposal'] = False
        tx.transaction['datetime'] = dt.strptime(transaction['created_at'], '%Y-%m-%dT%H:%M:%SZ')
        tx.transaction['initial_asset_quantity'] = abs(float(transaction['amount']['amount']))
        tx.transaction['initial_asset_currency'] = asset
        tx.transaction['initial_asset_location'] = 'Coinbase Pro'
        tx.transaction['initial_asset_address'] = None
        tx.transaction['price'] = None
        tx.transaction['final_asset_quantity'] = abs(float(transaction['amount']['amount']))
        tx.transaction['final_asset_currency'] = asset
        tx.transaction['final_asset_gbp'] = None
        tx.transaction['final_asset_location'] = None
        tx.transaction['final_asset_address'] = None
        tx.transaction['fee_type'] = 'transfer'
        tx.transaction['fee_quantity'] = None
        tx.transaction['fee_currency'] = None
        tx.transaction['fee_gbp'] = None
        tx.transaction['source_transaction_id'] = transaction['id']
        tx.transaction['source_trade_id'] = transaction['application']['id']

        return pd.DataFrame(tx.transaction, index=[0])

    @staticmethod
    def handle_fiat_deposit_transaction(asset, transaction):
        tx = Transaction()

        tx.transaction['asset'] = asset
        tx.transaction['type'] = transaction['type']
        tx.transaction['action'] = 'deposit_fiat'
        tx.transaction['disposal'] = False
        tx.transaction['datetime'] = dt.strptime(transaction['created_at'], '%Y-%m-%dT%H:%M:%SZ')
        tx.transaction['initial_asset_quantity'] = None
        tx.transaction['initial_asset_currency'] = None
        tx.transaction['initial_asset_location'] = None
        tx.transaction['initial_asset_address'] = None
        tx.transaction['price'] = None
        tx.transaction['final_asset_quantity'] = abs(float(transaction['amount']['amount']))
        tx.transaction['final_asset_currency'] = asset
        tx.transaction['final_asset_gbp'] = abs(float(transaction['native_amount']['amount']))
        tx.transaction['final_asset_location'] = 'Coinbase'
        tx.transaction['final_asset_address'] = None
        tx.transaction['fee_type'] = 'transfer'
        tx.transaction['fee_quantity'] = None
        tx.transaction['fee_currency'] = None
        tx.transaction['fee_gbp'] = None
        tx.transaction['source_transaction_id'] = transaction['id']
        tx.transaction['source_trade_id'] = None

        return pd.DataFrame(tx.transaction, index=[0])

    @staticmethod
    def handle_fiat_withdrawal_transaction(asset, transaction):
        tx = Transaction()

        tx.transaction['asset'] = asset
        tx.transaction['type'] = transaction['type']
        tx.transaction['action'] = 'withdraw_fiat'
        tx.transaction['disposal'] = False
        tx.transaction['datetime'] = dt.strptime(transaction['created_at'], '%Y-%m-%dT%H:%M:%SZ')
        tx.transaction['initial_asset_quantity'] = abs(float(transaction['amount']['amount']))
        tx.transaction['initial_asset_currency'] = asset
        tx.transaction['initial_asset_location'] = 'Coinbase'
        tx.transaction['initial_asset_address'] = None
        tx.transaction['price'] = None
        tx.transaction['final_asset_quantity'] = abs(float(transaction['amount']['amount']))
        tx.transaction['final_asset_currency'] = asset
        tx.transaction['final_asset_gbp'] = abs(float(transaction['native_amount']['amount']))
        tx.transaction['final_asset_location'] = None
        tx.transaction['final_asset_address'] = None
        tx.transaction['fee_type'] = 'transfer'
        tx.transaction['fee_quantity'] = None
        tx.transaction['fee_currency'] = None
        tx.transaction['fee_gbp'] = None
        tx.transaction['source_transaction_id'] = transaction['id']
        tx.transaction['source_trade_id'] = None

        return pd.DataFrame(tx.transaction, index=[0])

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
            'pro_withdrawal': Coinbase.handle_pro_withdrawal_transaction,
            'fiat_deposit': Coinbase.handle_fiat_deposit_transaction,
            'fiat_withdrawal': Coinbase.handle_fiat_withdrawal_transaction
        }

        transaction_dataframes = []
        for k, v in transaction_history.items():
            for i in v:
                if i['status'] == 'completed':
                    transaction_dataframes.append(transaction_type_function_map.get(i['type'])(k, i))

        if len(transaction_dataframes) > 0:
            transaction_df = pd.concat(transaction_dataframes)

            return transaction_df

    @staticmethod
    def create_buys_dataframe(buys_history):
        buys_dataframes = []
        for k, v in buys_history.items():
            for i in v:
                if i['status'] == 'completed':
                    tx = Transaction()

                    tx.transaction['asset'] = k
                    tx.transaction['type'] = 'buy'
                    tx.transaction['action'] = 'exchange_fiat_for_crypto'
                    tx.transaction['disposal'] = False
                    tx.transaction['datetime'] = dt.strptime(i['updated_at'], '%Y-%m-%dT%H:%M:%SZ')
                    tx.transaction['initial_asset_quantity'] = abs(float(i['total']['amount']))
                    tx.transaction['initial_asset_currency'] = i['total']['currency']
                    tx.transaction['initial_asset_location'] = 'Coinbase'
                    tx.transaction['initial_asset_address'] = None
                    tx.transaction['price'] = i['unit_price']['amount']
                    tx.transaction['final_asset_quantity'] = abs(float(i['amount']['amount']))
                    tx.transaction['final_asset_currency'] = k
                    tx.transaction['final_asset_gbp'] = abs(float(i['subtotal']['amount']))
                    tx.transaction['final_asset_location'] = 'Coinbase'
                    tx.transaction['final_asset_address'] = None
                    tx.transaction['fee_type'] = 'exchange'
                    tx.transaction['fee_quantity'] = i['fee']['amount']
                    tx.transaction['fee_currency'] = i['fee']['currency']
                    tx.transaction['fee_gbp'] = None
                    tx.transaction['source_transaction_id'] = i['id']
                    tx.transaction['source_trade_id'] = None

                    buys_dataframes.append(pd.DataFrame(tx.transaction, index=[0]))

        if len(buys_dataframes) > 0:
            buys_df = pd.concat(buys_dataframes)

            return buys_df

    @staticmethod
    def create_sells_dataframe(sells_history):
        sells_dataframes = []
        for k, v in sells_history.items():
            for i in v:
                if i['status'] == 'completed':
                    tx = Transaction()

                    tx.transaction['asset'] = k
                    tx.transaction['type'] = 'sell'
                    tx.transaction['action'] = 'exchange_crypto_for_fiat'
                    tx.transaction['disposal'] = True
                    tx.transaction['datetime'] = dt.strptime(i['updated_at'], '%Y-%m-%dT%H:%M:%SZ')
                    tx.transaction['initial_asset_quantity'] = abs(float(i['total']['amount']))
                    tx.transaction['initial_asset_currency'] = i['total']['currency']
                    tx.transaction['initial_asset_location'] = 'Coinbase'
                    tx.transaction['initial_asset_address'] = None
                    tx.transaction['price'] = i['unit_price']['amount']
                    tx.transaction['final_asset_quantity'] = abs(float(i['amount']['amount']))
                    tx.transaction['final_asset_currency'] = k
                    tx.transaction['final_asset_gbp'] = abs(float(i['subtotal']['amount']))
                    tx.transaction['final_asset_location'] = 'Coinbase'
                    tx.transaction['final_asset_address'] = None
                    tx.transaction['fee_type'] = 'exchange'
                    tx.transaction['fee_quantity'] = i['fee']['amount']
                    tx.transaction['fee_currency'] = i['fee']['currency']
                    tx.transaction['fee_gbp'] = None
                    tx.transaction['source_transaction_id'] = i['id']
                    tx.transaction['source_trade_id'] = None

                    sells_dataframes.append(pd.DataFrame(tx.transaction, index=[0]))

        if len(sells_dataframes) > 0:
            sells_df = pd.concat(sells_dataframes)

            return sells_df


if __name__ == '__main__':
    x = Coinbase()
    x.get_coinbase_transactions()
