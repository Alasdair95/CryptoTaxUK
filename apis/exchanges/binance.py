import os
import requests
import time
import pandas as pd
from datetime import timedelta
from datetime import datetime as dt

from apis.authentication import BinanceAuth
from apis.helpers import Transaction


class Binance:
    def __init__(self):
        self.api_key = os.environ.get('BINANCE_API_KEY')
        self.api_secret = os.environ.get('BINANCE_API_SECRET')
        self.base_url = 'https://api.binance.com'

    def get_binance_transactions(self):
        # Get deposits
        deposits = self.get_deposits()

        deposit_dataframes = []
        for deposit in deposits:
            deposit_dataframes.append(Binance.create_deposit_datarame(deposit))

        df_deposits = pd.concat(deposit_dataframes)

        # Get withdrawals
        withdrawals = self.get_withdrawals()

        withdrawal_dataframes = []
        for withdrawal in withdrawals:
            withdrawal_dataframes.append(Binance.create_withdrawal_datarame(withdrawal))

        df_withdrawals = pd.concat(withdrawal_dataframes)

        # Get trades
        symbols = self.get_symbols()

        count = 0
        df_trades_list = []
        for symbol in symbols:
            trades = self.get_symbol_trades(symbol['symbol'])
            time.sleep(1)

            if len(trades) > 0:
                print(f'{count}/{len(symbols)} checked.')
                print(f"{symbol['symbol']}: {len(trades)} trades")
                for trade in trades:
                    df_trades_list.append(Binance.create_trades_dataframes(symbol, trade))

            count += 1

        df_trades = pd.concat(df_trades_list)

        # Get dust transactions
        dust_transactions = self.get_dust_transactions()

        dust_transactions_dataframes = []
        for d in dust_transactions:
            dust_transactions_dataframes.append(Binance.create_dust_transaction_datarame(d))

        df_dust_transactions = pd.concat(dust_transactions_dataframes)

        # df_trades = pd.read_csv(r"C:\Users\alasd\Documents\Projects Misc\binance.csv")
        # df_trades['datetime'] = pd.to_datetime(df_trades['datetime'])

        df_final = pd.concat([df_deposits, df_withdrawals, df_trades, df_dust_transactions])

        df_final.sort_values(by='datetime', inplace=True)

        return None

    def get_symbols(self):
        path = '/api/v3/exchangeInfo'

        r = requests.get(self.base_url + path).json()

        # symbols = [i['symbol'] for i in r['symbols']]

        return r['symbols']

    def get_symbol_trades(self, symbol):
        path = '/api/v3/myTrades'

        c = BinanceAuth(self.api_key, self.api_secret)
        headers = c.get_request_headers()

        params = {
            'symbol': symbol,
            'recvWindow': 20000,
            'timestamp': int(time.time() * 1000)
        }

        params['signature'] = c.get_request_signature(params)

        r = requests.get(self.base_url + path, headers=headers, params=params).json()

        return r

    @staticmethod
    def get_action(symbol, trade_action):
        if symbol['quoteAsset'] in ['GBP', 'EUR', 'USD']:
            if trade_action == 'buy':
                return 'exchange_fiat_for_crypto'
            else:
                return 'exchange_crypto_for_fiat'
        else:
            return 'exchange_crypto_for_crypto'

    @staticmethod
    def is_disposal(symbol, trade_action):
        if symbol['quoteAsset'] in ['GBP', 'EUR', 'USD']:
            if trade_action == 'buy':
                return False
            else:
                return True
        else:
            if trade_action == 'buy':
                return False
            else:
                return True

    @staticmethod
    def create_trades_dataframes(symbol, trade):
        if trade['isBuyer']:
            trade_action = 'buy'
        else:
            trade_action = 'sell'

        # Create buy transaction
        tx_buy = Transaction()

        tx_buy.transaction['asset'] = symbol['baseAsset']
        tx_buy.transaction['action'] = Binance.get_action(symbol, trade_action)
        tx_buy.transaction['type'] = None
        tx_buy.transaction['disposal'] = Binance.is_disposal(symbol, trade_action)
        tx_buy.transaction['datetime'] = dt.fromtimestamp(int(str(trade['time'])[:-3]))
        if trade_action == 'buy':
            tx_buy.transaction['initial_asset_quantity'] = trade['quoteQty']
            tx_buy.transaction['initial_asset_currency'] = symbol['quoteAsset']
            tx_buy.transaction['initial_asset_address'] = None
            tx_buy.transaction['final_asset_quantity'] = trade['qty']
            tx_buy.transaction['final_asset_currency'] = symbol['baseAsset']
        else:
            tx_buy.transaction['initial_asset_quantity'] = trade['qty']
            tx_buy.transaction['initial_asset_currency'] = symbol['baseAsset']
            tx_buy.transaction['initial_asset_address'] = None
            tx_buy.transaction['final_asset_quantity'] = trade['quoteQty']
            tx_buy.transaction['final_asset_currency'] = symbol['quoteAsset']
        tx_buy.transaction['price'] = trade['price']
        tx_buy.transaction['final_asset_gbp'] = None
        tx_buy.transaction['initial_asset_location'] = 'Binance'
        tx_buy.transaction['final_asset_location'] = 'Binance'
        tx_buy.transaction['final_asset_address'] = None
        tx_buy.transaction['fee_type'] = 'exchange'
        tx_buy.transaction['fee_quantity'] = trade['commission']
        tx_buy.transaction['fee_currency'] = trade['commissionAsset']
        tx_buy.transaction['fee_gbp'] = None
        tx_buy.transaction['source_transaction_id'] = trade['id']
        tx_buy.transaction['source_trade_id'] = None

        df_tx_buy = pd.DataFrame(tx_buy.transaction, index=[0])

        # Flip trade_action
        if trade_action == 'buy':
            trade_action = 'sell'
        else:
            trade_action = 'buy'

        # Create sell transaction
        tx_sell = Transaction()

        tx_sell.transaction['asset'] = symbol['quoteAsset']
        tx_sell.transaction['action'] = Binance.get_action(symbol, trade_action)
        tx_sell.transaction['type'] = None
        tx_sell.transaction['disposal'] = Binance.is_disposal(symbol, trade_action)
        tx_sell.transaction['datetime'] = dt.fromtimestamp(int(str(trade['time'])[:-3]))
        if trade_action == 'buy':
            tx_sell.transaction['initial_asset_quantity'] = trade['quoteQty']
            tx_sell.transaction['initial_asset_currency'] = symbol['quoteAsset']
            tx_sell.transaction['initial_asset_address'] = None
            tx_sell.transaction['final_asset_quantity'] = trade['qty']
            tx_sell.transaction['final_asset_currency'] = symbol['baseAsset']
        else:
            tx_sell.transaction['initial_asset_quantity'] = trade['qty']
            tx_sell.transaction['initial_asset_currency'] = symbol['baseAsset']
            tx_sell.transaction['initial_asset_address'] = None
            tx_sell.transaction['final_asset_quantity'] = trade['quoteQty']
            tx_sell.transaction['final_asset_currency'] = symbol['quoteAsset']
        tx_sell.transaction['price'] = trade['price']
        tx_sell.transaction['final_asset_gbp'] = None
        tx_sell.transaction['initial_asset_location'] = 'Binance'
        tx_sell.transaction['final_asset_location'] = 'Binance'
        tx_sell.transaction['final_asset_address'] = None
        tx_sell.transaction['fee_type'] = 'exchange'
        tx_sell.transaction['fee_quantity'] = trade['commission']
        tx_sell.transaction['fee_currency'] = trade['commissionAsset']
        tx_sell.transaction['fee_gbp'] = None
        tx_sell.transaction['source_transaction_id'] = trade['id']
        tx_sell.transaction['source_trade_id'] = None

        df_tx_sell = pd.DataFrame(tx_sell.transaction, index=[0])

        return pd.concat([df_tx_buy, df_tx_sell])

    def get_deposits(self):
        path = '/wapi/v3/depositHistory.html'

        start_timestamp = int(dt.timestamp(dt.strptime('2017-11-01', '%Y-%m-%d')))
        end_timestamp = int(dt.timestamp(dt.strptime('2017-11-01', '%Y-%m-%d') + timedelta(days=90)))

        deposits = []
        while end_timestamp < dt.timestamp(dt.now()):
            c = BinanceAuth(self.api_key, self.api_secret)
            headers = c.get_request_headers()

            params = {
                'startTime': start_timestamp*1000,
                'endTime': end_timestamp*1000,
                'recvWindow': 20000,
                'timestamp': int(time.time() * 1000)
            }

            params['signature'] = c.get_request_signature(params)

            r = requests.get(self.base_url + path, headers=headers, params=params).json()

            if len(r['depositList']) > 0:
                for deposit in r['depositList']:
                    deposits.append(deposit)

            start_timestamp = end_timestamp
            end_timestamp = end_timestamp + 7776000

        return deposits

    def get_withdrawals(self):
        path = '/wapi/v3/withdrawHistory.html'

        start_timestamp = int(dt.timestamp(dt.strptime('2017-11-01', '%Y-%m-%d')))
        end_timestamp = int(dt.timestamp(dt.strptime('2017-11-01', '%Y-%m-%d') + timedelta(days=90)))

        withdrawals = []
        while end_timestamp < dt.timestamp(dt.now()):
            c = BinanceAuth(self.api_key, self.api_secret)
            headers = c.get_request_headers()

            params = {
                'startTime': start_timestamp*1000,
                'endTime': end_timestamp*1000,
                'recvWindow': 20000,
                'timestamp': int(time.time() * 1000)
            }

            params['signature'] = c.get_request_signature(params)

            r = requests.get(self.base_url + path, headers=headers, params=params).json()

            if len(r['withdrawList']) > 0:
                for withdrawal in r['withdrawList']:
                    withdrawals.append(withdrawal)

            start_timestamp = end_timestamp
            end_timestamp = end_timestamp + 7776000

        return withdrawals

    def get_dust_transactions(self):
        path = '/wapi/v3/userAssetDribbletLog.html'

        c = BinanceAuth(self.api_key, self.api_secret)
        headers = c.get_request_headers()

        params = {
            'recvWindow': 20000,
            'timestamp': int(time.time() * 1000)
        }

        params['signature'] = c.get_request_signature(params)

        r = requests.get(self.base_url + path, headers=headers, params=params).json()

        dust_transactions = []
        for i in r['results']['rows']:
            for j in i['logs']:
                dust_transactions.append(j)

        return dust_transactions

    @staticmethod
    def create_deposit_datarame(deposit):
        tx = Transaction()

        tx.transaction['asset'] = deposit['asset']
        if deposit['asset'] in ['GBP', 'EUR', 'USD']:
            tx.transaction['action'] = 'deposit_fiat'
        else:
            tx.transaction['action'] = 'deposit_crypto'
        tx.transaction['disposal'] = False
        tx.transaction['datetime'] = dt.fromtimestamp(deposit['insertTime']/1000)
        tx.transaction['initial_asset_quantity'] = deposit['amount']
        tx.transaction['initial_asset_currency'] = deposit['asset']
        tx.transaction['initial_asset_location'] = None
        tx.transaction['initial_asset_address'] = None
        tx.transaction['price'] = None
        tx.transaction['final_asset_quantity'] = deposit['amount']
        tx.transaction['final_asset_currency'] = deposit['asset']
        tx.transaction['final_asset_gbp'] = None
        tx.transaction['final_asset_location'] = 'Binance'
        tx.transaction['final_asset_address'] = deposit['address']
        tx.transaction['fee_type'] = 'exchange'
        tx.transaction['fee_quantity'] = None
        tx.transaction['fee_currency'] = None
        tx.transaction['fee_gbp'] = None
        tx.transaction['source_transaction_id'] = deposit['txId']
        tx.transaction['source_trade_id'] = None

        return pd.DataFrame(tx.transaction, index=[0])

    @staticmethod
    def create_withdrawal_datarame(withdrawal):
        tx = Transaction()

        tx.transaction['asset'] = withdrawal['asset']
        if withdrawal['asset'] in ['GBP', 'EUR', 'USD']:
            tx.transaction['action'] = 'withdraw_fiat'
        else:
            tx.transaction['action'] = 'withdraw_crypto'
        tx.transaction['disposal'] = False
        tx.transaction['datetime'] = dt.fromtimestamp(withdrawal['applyTime'] / 1000)
        tx.transaction['initial_asset_quantity'] = withdrawal['amount']
        tx.transaction['initial_asset_currency'] = withdrawal['asset']
        tx.transaction['initial_asset_location'] = 'Binance'
        tx.transaction['initial_asset_address'] = None
        tx.transaction['price'] = None
        tx.transaction['final_asset_quantity'] = withdrawal['amount']
        tx.transaction['final_asset_currency'] = withdrawal['asset']
        tx.transaction['final_asset_gbp'] = None
        tx.transaction['final_asset_location'] = None
        tx.transaction['final_asset_address'] = withdrawal['address']
        tx.transaction['fee_type'] = 'withdrawal'
        tx.transaction['fee_quantity'] = withdrawal['transactionFee']
        tx.transaction['fee_currency'] = withdrawal['asset']
        tx.transaction['fee_gbp'] = None
        tx.transaction['source_transaction_id'] = withdrawal['id']
        tx.transaction['source_trade_id'] = None

        return pd.DataFrame(tx.transaction, index=[0])

    @staticmethod
    def create_dust_transaction_datarame(dust_transaction):
        # Create buy transaction
        tx_buy = Transaction()

        tx_buy.transaction['asset'] = 'BNB'
        tx_buy.transaction['action'] = 'exchange_crypto_for_crypto'
        tx_buy.transaction['type'] = None
        tx_buy.transaction['disposal'] = False
        tx_buy.transaction['datetime'] = dust_transaction['operateTime']
        tx_buy.transaction['initial_asset_quantity'] = dust_transaction['amount']
        tx_buy.transaction['initial_asset_currency'] = dust_transaction['fromAsset']
        tx_buy.transaction['initial_asset_location'] = 'Binance'
        tx_buy.transaction['initial_asset_address'] = None
        tx_buy.transaction['price'] = round(float(dust_transaction['transferedAmount']) / float(dust_transaction['amount']), 8)
        tx_buy.transaction['final_asset_quantity'] = dust_transaction['transferedAmount']
        tx_buy.transaction['final_asset_currency'] = 'BNB'
        tx_buy.transaction['final_asset_gbp'] = None
        tx_buy.transaction['final_asset_location'] = 'Binance'
        tx_buy.transaction['final_asset_address'] = None
        tx_buy.transaction['fee_type'] = 'exchange'
        tx_buy.transaction['fee_quantity'] = dust_transaction['serviceChargeAmount']
        tx_buy.transaction['fee_currency'] = 'BNB'
        tx_buy.transaction['fee_gbp'] = None
        tx_buy.transaction['source_transaction_id'] = dust_transaction['tranId']
        tx_buy.transaction['source_trade_id'] = None

        df_tx_buy = pd.DataFrame(tx_buy.transaction, index=[0])

        # Create sell transaction
        tx_sell = Transaction()

        tx_sell.transaction['asset'] = dust_transaction['fromAsset']
        tx_sell.transaction['action'] = 'exchange_crypto_for_crypto'
        tx_sell.transaction['type'] = None
        tx_sell.transaction['disposal'] = True
        tx_sell.transaction['datetime'] = dust_transaction['operateTime']
        tx_sell.transaction['initial_asset_quantity'] = dust_transaction['amount']
        tx_sell.transaction['initial_asset_currency'] = dust_transaction['fromAsset']
        tx_sell.transaction['initial_asset_location'] = 'Binance'
        tx_sell.transaction['initial_asset_address'] = None
        tx_sell.transaction['price'] = round(float(dust_transaction['transferedAmount']) / float(dust_transaction['amount']), 8)
        tx_sell.transaction['final_asset_quantity'] = dust_transaction['transferedAmount']
        tx_sell.transaction['final_asset_currency'] = 'BNB'
        tx_sell.transaction['final_asset_gbp'] = None
        tx_sell.transaction['final_asset_location'] = 'Binance'
        tx_sell.transaction['final_asset_address'] = None
        tx_sell.transaction['fee_type'] = 'exchange'
        tx_sell.transaction['fee_quantity'] = dust_transaction['serviceChargeAmount']
        tx_sell.transaction['fee_currency'] = 'BNB'
        tx_sell.transaction['fee_gbp'] = None
        tx_sell.transaction['source_transaction_id'] = dust_transaction['tranId']
        tx_sell.transaction['source_trade_id'] = None

        df_tx_sell = pd.DataFrame(tx_sell.transaction, index=[0])

        return pd.concat([df_tx_buy, df_tx_sell])


if __name__ == '__main__':
    x = Binance()
    x.get_binance_transactions()
    # x.get_dust_transactions()

