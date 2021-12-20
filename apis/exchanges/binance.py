import os
import requests
import time
import json
import pandas as pd
from datetime import timedelta
from datetime import datetime as dt

from apis.authentication import BinanceAuth
from apis.helpers import Transaction, BinanceConvertToGBP


class Binance:
    def __init__(self):
        self.api_key = os.environ.get('BINANCE_API_KEY')
        self.api_secret = os.environ.get('BINANCE_API_SECRET')
        self.base_url = 'https://api.binance.com'
        self.source_transactions_save_path = 'data/source_transactions/binance.csv'

    def get_binance_transactions(self):
        # Get existing transactions dataframe if it exists
        if os.path.isfile(self.source_transactions_save_path):
            df = pd.read_csv(self.source_transactions_save_path)
            most_recent_transaction = df['datetime'].tolist()[-1]
            df['datetime'] = pd.to_datetime(df['datetime'])
        else:
            df = pd.DataFrame(Transaction().transaction, index=[0]).dropna()
            most_recent_transaction = None

        # Get deposits
        deposits = self.get_deposits(start=most_recent_transaction)

        deposit_dataframes = []
        if len(deposits) > 0:
            for deposit in deposits:
                deposit_dataframes.append(Binance.create_deposit_dataframe(deposit))
            df_deposits = pd.concat(deposit_dataframes)
        else:
            df_deposits = pd.DataFrame(Transaction().transaction, index=[0]).dropna()

        # Get withdrawals
        withdrawals = self.get_withdrawals(start=most_recent_transaction)

        withdrawal_dataframes = []
        if len(withdrawals) > 0:
            for withdrawal in withdrawals:
                withdrawal_dataframes.append(Binance.create_withdrawal_dataframe(withdrawal))
            df_withdrawals = pd.concat(withdrawal_dataframes)
        else:
            df_withdrawals = pd.DataFrame(Transaction().transaction, index=[0]).dropna()

        # Get dust transactions
        dust_transactions = self.get_dust_transactions(start=most_recent_transaction)

        dust_transactions_dataframes = []
        if len(dust_transactions) > 0:
            for d in dust_transactions:
                dust_transactions_dataframes.append(Binance.create_dust_transaction_dataframe(d))
            df_dust_transactions = pd.concat(dust_transactions_dataframes)
        else:
            df_dust_transactions = pd.DataFrame(Transaction().transaction, index=[0]).dropna()

        # Get dividend transactions
        dividend_transactions = self.get_dividend_transactions(start=most_recent_transaction)

        dividend_transactions_dataframes = []
        if len(dividend_transactions):
            for d in dividend_transactions:
                dividend_transactions_dataframes.append(Binance.create_dividend_transaction_dataframe(d))
            df_dividend_transactions = pd.concat(dividend_transactions_dataframes)
        else:
            df_dividend_transactions = pd.DataFrame(Transaction().transaction, index=[0]).dropna()

        # Get trades
        symbols = self.get_symbols()

        count = 0
        df_trades_list = []
        for symbol in symbols:
            trades = self.get_symbol_trades(symbol['symbol'], start=most_recent_transaction)
            time.sleep(1)

            if len(trades) > 0:
                print(f'{count}/{len(symbols)} checked.')
                print(f"{symbol['symbol']}: {len(trades)} trades")
                for trade in trades:
                    df_trades_list.append(Binance.create_trades_dataframes(symbol, trade))
            # else:
            #     df_trades = pd.DataFrame(Transaction().transaction, index=[0]).dropna()
            count += 1

        if len(df_trades_list) > 0:
            df_trades = pd.concat(df_trades_list)
            # df_trades = pd.read_csv(r"C:\Users\alasd\Documents\Projects Misc\binance.csv")

            df_final = pd.concat([df_deposits, df_withdrawals, df_trades, df_dust_transactions, df_dividend_transactions])
            df_final['datetime'] = pd.to_datetime(df_final['datetime'])

            df_final.sort_values(by='datetime', inplace=True)

            # df_final = pd.read_csv(r"C:\Users\alasd\Documents\Projects Misc\binance_temp.csv")

            # Check whether cached_rates_gbp.json exists
            if not os.path.isfile('data/cached_gbp_rates.json'):
                with open('data/cached_gbp_rates.json', 'w') as f:
                    json.dump({}, f)

            # Read cached crypto/gbp rates
            with open('data/cached_gbp_rates.json') as j:
                cached_rates = json.load(j)

            # GBP conversions
            # Loop through and get GBP values where missing
            final_asset_gbp = []
            fee_gbp = []
            count_api = 0
            count_cache = 0
            for row in df_final.itertuples():
                # Calculate GBP value for all disposals
                if row.action in ['exchange_fiat_for_crypto', 'exchange_crypto_for_fiat', 'exchange_crypto_for_crypto']:
                    if row.final_asset_currency == 'GBP':
                        final_asset_gbp.append(row.final_asset_quantity)
                    else:
                        asset = row.final_asset_currency
                        datetime = dt.strptime(str(row.datetime), '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d %H:%M:00')
                        quantity = row.final_asset_quantity

                        # Check whether we have already cached the GBP rate for the asset and datetime in question
                        if asset in cached_rates.keys():
                            if not cached_rates[asset].get(datetime):
                                # Convert to GBP
                                c = BinanceConvertToGBP(asset, datetime, quantity)
                                quantity_gbp = c.convert_to_gbp()
                                final_asset_gbp.append(quantity_gbp)
                                # Calculate the asset GBP rate at this datetime and cache
                                rate_gbp = quantity_gbp / float(quantity)
                                cached_rates[asset][datetime] = rate_gbp
                                count_api += 1
                            else:
                                rate_gbp = cached_rates[asset].get(datetime)
                                quantity_gbp = rate_gbp * float(quantity)
                                final_asset_gbp.append(quantity_gbp)
                                count_cache += 1
                        else:
                            c = BinanceConvertToGBP(asset, datetime, quantity)
                            quantity_gbp = c.convert_to_gbp()
                            final_asset_gbp.append(quantity_gbp)
                            rate_gbp = quantity_gbp / float(quantity)
                            cached_rates[asset] = {}
                            cached_rates[asset][datetime] = rate_gbp
                            count_api += 1
                else:
                    final_asset_gbp.append(None)

                # Calculate all fees in GBP
                if not pd.isna(row.fee_currency):
                    if any([row.fee_currency == 'GBP', row.fee_quantity == 0.0]):
                        fee_gbp.append(row.fee_quantity)
                    else:
                        asset = row.fee_currency
                        datetime = dt.strptime(str(row.datetime), '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d %H:%M:00')
                        quantity = row.fee_quantity

                        # Check whether we have already cached the GBP rate for the asset and datetime in question
                        if asset in cached_rates.keys():
                            if not cached_rates[asset].get(datetime):
                                # Convert to GBP
                                c = BinanceConvertToGBP(asset, datetime, quantity)
                                quantity_gbp = c.convert_to_gbp()
                                fee_gbp.append(quantity_gbp)
                                # Calculate the asset GBP rate at this datetime and cache
                                rate_gbp = quantity_gbp / float(quantity)
                                cached_rates[asset][datetime] = rate_gbp
                                count_api += 1
                            else:
                                rate_gbp = cached_rates[asset].get(datetime)
                                quantity_gbp = rate_gbp * float(row.fee_quantity)
                                fee_gbp.append(quantity_gbp)
                                count_cache += 1
                        else:
                            c = BinanceConvertToGBP(asset, datetime, quantity)
                            quantity_gbp = c.convert_to_gbp()
                            fee_gbp.append(quantity_gbp)
                            rate_gbp = quantity_gbp / float(quantity)
                            cached_rates[asset] = {}
                            cached_rates[asset][datetime] = rate_gbp
                            count_api += 1
                else:
                    fee_gbp.append(None)

                print(f'{count_api} : API')
                print(f'{count_cache}   : Cache')

            print(f'Count API:\t {count_api}')
            print(f'Count cache:\t {count_cache}')

            # Save cached_rates back to json file for quicker conversions on next run
            with open('data/cached_gbp_rates.json', 'w', encoding='utf-8') as f:
                json.dump(cached_rates, f, ensure_ascii=False, indent=4)

            df_final['final_asset_gbp'] = final_asset_gbp
            df_final['fee_gbp'] = fee_gbp
        else:
            df_final = pd.DataFrame(Transaction().transaction, index=[0]).dropna()

        # Add all new transactions to the existing dataframe
        df_full = pd.concat([df, df_final]).sort_values(by='datetime')

        return df_full

    def get_symbols(self):
        path = '/api/v3/exchangeInfo'

        r = requests.get(self.base_url + path).json()

        return r['symbols']

    def get_symbol_trades(self, symbol, start=None):
        path = '/api/v3/myTrades'

        if not start:
            initial_timestamp = int(dt.timestamp(dt.strptime('2017-11-01', '%Y-%m-%d')))
        else:
            initial_timestamp = int(dt.timestamp(dt.strptime(start, '%Y-%m-%d %H:%M:%S')))

        c = BinanceAuth(self.api_key, self.api_secret)
        headers = c.get_request_headers()

        params = {
            'symbol': symbol,
            'limit': 1000,
            'recvWindow': 20000,
            'timestamp': int(time.time() * 1000)
        }

        params['signature'] = c.get_request_signature(params)

        r = requests.get(self.base_url + path, headers=headers, params=params).json()

        if type(r) == list:  # TODO: while loop?
            if len(r) == params['limit']:
                # TODO: add pagination logic using params.fromId
                pass

        # If we get a bad response because we're hitting the API too much, wait 30s before trying again
        if type(r) != list:  # TODO: Change to while loop
            time.sleep(30)
            c = BinanceAuth(self.api_key, self.api_secret)
            headers = c.get_request_headers()

            params = {
                'symbol': symbol,
                'recvWindow': 20000,
                'timestamp': int(time.time() * 1000)
            }

            params['signature'] = c.get_request_signature(params)

            r = requests.get(self.base_url + path, headers=headers, params=params).json()

        symbol_trades = []
        if type(r) == list:
            if len(r) > 0:
                for i in r:
                    if i['time'] > (1000 * initial_timestamp):
                        symbol_trades.append(i)

        return symbol_trades

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
            tx_sell.transaction['initial_asset_quantity'] = trade['qty']
            tx_sell.transaction['initial_asset_currency'] = symbol['baseAsset']
            tx_sell.transaction['initial_asset_address'] = None
            tx_sell.transaction['final_asset_quantity'] = trade['quoteQty']
            tx_sell.transaction['final_asset_currency'] = symbol['quoteAsset']
        else:
            tx_sell.transaction['initial_asset_quantity'] = trade['quoteQty']
            tx_sell.transaction['initial_asset_currency'] = symbol['quoteAsset']
            tx_sell.transaction['initial_asset_address'] = None
            tx_sell.transaction['final_asset_quantity'] = trade['qty']
            tx_sell.transaction['final_asset_currency'] = symbol['baseAsset']
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

    def get_deposits(self, start=None):
        # path = '/wapi/v3/depositHistory.html'
        path = '/sapi/v1/capital/deposit/hisrec'

        if not start:
            initial_timestamp = int(dt.timestamp(dt.strptime('2017-11-01', '%Y-%m-%d')))
            start_timestamp = int(dt.timestamp(dt.strptime('2017-11-01', '%Y-%m-%d')))
            end_timestamp = int(dt.timestamp(dt.strptime('2017-11-01', '%Y-%m-%d') + timedelta(days=90)))
        else:
            initial_timestamp = int(dt.timestamp(dt.strptime(start, '%Y-%m-%d %H:%M:%S')))
            start_timestamp = int(dt.timestamp(dt.strptime(start, '%Y-%m-%d %H:%M:%S')))
            end_timestamp = int(dt.timestamp(dt.strptime(start, '%Y-%m-%d %H:%M:%S') + timedelta(days=90)))

        deposits = []
        while start_timestamp < dt.timestamp(dt.now()):
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
                    if deposit['insertTime'] > (1000*initial_timestamp):
                        deposits.append(deposit)

            start_timestamp = end_timestamp
            end_timestamp = end_timestamp + 7776000

        return deposits

    def get_withdrawals(self, start=None):
        path = '/wapi/v3/withdrawHistory.html'

        if not start:
            initial_timestamp = int(dt.timestamp(dt.strptime('2017-11-01', '%Y-%m-%d')))
            start_timestamp = int(dt.timestamp(dt.strptime('2017-11-01', '%Y-%m-%d')))
            end_timestamp = int(dt.timestamp(dt.strptime('2017-11-01', '%Y-%m-%d') + timedelta(days=90)))
        else:
            initial_timestamp = int(dt.timestamp(dt.strptime(start, '%Y-%m-%d %H:%M:%S')))
            start_timestamp = int(dt.timestamp(dt.strptime(start, '%Y-%m-%d %H:%M:%S')))
            end_timestamp = int(dt.timestamp(dt.strptime(start, '%Y-%m-%d %H:%M:%S') + timedelta(days=90)))

        withdrawals = []
        while start_timestamp < dt.timestamp(dt.now()):
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
                    if withdrawal['applyTime'] > (1000*initial_timestamp):
                        withdrawals.append(withdrawal)

            start_timestamp = end_timestamp
            end_timestamp = end_timestamp + 7776000

        return withdrawals

    def get_dust_transactions(self, start=None):
        path = '/wapi/v3/userAssetDribbletLog.html'

        if not start:
            start_timestamp = dt.strptime('2017-11-01', '%Y-%m-%d')
        else:
            start_timestamp = dt.strptime(start, '%Y-%m-%d %H:%M:%S')

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
                if dt.strptime(j['operateTime'], '%Y-%m-%d %H:%M:%S') > start_timestamp:
                    dust_transactions.append(j)

        return dust_transactions

    def get_dividend_transactions(self, start=None):
        path = '/sapi/v1/asset/assetDividend'

        if not start:
            start_timestamp = int(dt.timestamp(dt.strptime('2017-11-01', '%Y-%m-%d')))
        else:
            start_timestamp = int(dt.timestamp(dt.strptime(start, '%Y-%m-%d %H:%M:%S')))

        c = BinanceAuth(self.api_key, self.api_secret)
        headers = c.get_request_headers()

        params = {
            'limit': 500,
            'recvWindow': 20000,
            'timestamp': int(time.time() * 1000)
        }

        params['signature'] = c.get_request_signature(params)

        r = requests.get(self.base_url + path, headers=headers, params=params).json()

        dividend_transactions = []
        for i in r['rows']:
            if i['divTime'] > (1000 * start_timestamp):
                dividend_transactions.append(i)

        if r['total'] == params['limit']:
            # TODO: Add code to handle pagination when required
            print('Need to handle dividend pagination!')

        return dividend_transactions

    @staticmethod
    def create_deposit_dataframe(deposit):
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
    def create_withdrawal_dataframe(withdrawal):
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
    def create_dust_transaction_dataframe(dust_transaction):
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

    @staticmethod
    def create_dividend_transaction_dataframe(dividend_transaction):
        tx = Transaction()

        tx.transaction['asset'] = dividend_transaction['asset']
        tx.transaction['action'] = 'airdrop'
        tx.transaction['disposal'] = False
        tx.transaction['datetime'] = dt.fromtimestamp(dividend_transaction['divTime'] / 1000)
        tx.transaction['initial_asset_quantity'] = dividend_transaction['amount']
        tx.transaction['initial_asset_currency'] = dividend_transaction['asset']
        tx.transaction['initial_asset_location'] = 'Binance'
        tx.transaction['initial_asset_address'] = None
        tx.transaction['price'] = None
        tx.transaction['final_asset_quantity'] = dividend_transaction['amount']
        tx.transaction['final_asset_currency'] = dividend_transaction['asset']
        tx.transaction['final_asset_gbp'] = None
        tx.transaction['final_asset_location'] = None
        tx.transaction['final_asset_address'] = None
        tx.transaction['fee_type'] = None
        tx.transaction['fee_quantity'] = None
        tx.transaction['fee_currency'] = None
        tx.transaction['fee_gbp'] = None
        tx.transaction['source_transaction_id'] = dividend_transaction['id']
        tx.transaction['source_trade_id'] = None

        return pd.DataFrame(tx.transaction, index=[0])


if __name__ == '__main__':
    x = Binance()
    # x.get_binance_transactions()
    # start = None
    # start = '2020-08-20 18:45:38'
    # x.get_symbol_trades(symbol='ETHBTC', start=start)
    x.get_deposits()
