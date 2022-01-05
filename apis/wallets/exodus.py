import os
import json
import pandas as pd
from datetime import datetime as dt

from apis.helpers import Transaction, BinanceConvertToGBP, CoinAPIConvertToGBP


class Exodus:
    def __init__(self):
        self.path = os.path.join(os.environ["HOMEDRIVE"], os.environ["HOMEPATH"], "Desktop\\exodus-exports\\")

    def get_exodus_transactions(self):
        df = self.get_csv()

        transactions = []
        for row in df.itertuples():
            transactions.append(Exodus.create_transaction_dataframe(row))

        df_transactions = pd.concat(transactions)

        df_transactions.sort_values(by='datetime', inplace=True)

        # Check whether cached_rates_gbp.json exists
        if not os.path.isfile('data/cached_gbp_rates.json'):
            with open('data/cached_gbp_rates.json', 'w') as f:
                json.dump({}, f)

        # Read cached crypto/gbp rates
        with open('data/cached_gbp_rates.json') as j:
            cached_rates = json.load(j)

        # Loop through and get GBP values where missing
        final_asset_gbp = []
        fee_gbp = []
        count_api = 0
        count_cache = 0
        for row in df_transactions.itertuples():
            # Calculate GBP value for all disposals
            if row.action in ['exchange_fiat_for_crypto', 'exchange_crypto_for_fiat', 'exchange_crypto_for_crypto']:
                if pd.isna(row.final_asset_gbp):
                    if not pd.isna(row.final_asset_gbp):
                        final_asset_gbp.append(None)
                    elif row.final_asset_currency == 'GBP':
                        final_asset_gbp.append(row.final_asset_quantity)
                    else:
                        asset = row.final_asset_currency
                        datetime = dt.strptime(str(row.datetime), '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d %H:%M:00')
                        quantity = row.final_asset_quantity
                        # Check whether we have already cached the GBP rate for the asset and datetime in question
                        if asset in cached_rates.keys():
                            if not cached_rates[asset].get(datetime):
                                try:
                                    c = BinanceConvertToGBP(asset, datetime, quantity)
                                    gbp_value = c.convert_to_gbp()
                                except IndexError:
                                    c = CoinAPIConvertToGBP(asset, datetime, quantity)
                                    gbp_value = c.convert_to_gbp()
                                final_asset_gbp.append(gbp_value)
                                if float(quantity):
                                    rate_gbp = gbp_value / float(quantity)
                                    cached_rates[asset] = {}
                                    cached_rates[asset][datetime] = rate_gbp
                                count_api += 1
                            else:
                                rate_gbp = cached_rates[asset].get(datetime)
                                quantity_gbp = rate_gbp * float(quantity)
                                final_asset_gbp.append(quantity_gbp)
                                count_cache += 1
                        else:
                            try:
                                c = BinanceConvertToGBP(asset, datetime, quantity)
                                gbp_value = c.convert_to_gbp()
                            except IndexError:
                                c = CoinAPIConvertToGBP(asset, datetime, quantity)
                                gbp_value = c.convert_to_gbp()
                            final_asset_gbp.append(gbp_value)
                            if float(quantity):
                                rate_gbp = gbp_value / float(quantity)
                                cached_rates[asset] = {}
                                cached_rates[asset][datetime] = rate_gbp
                            count_api += 1
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
                    datetime = dt.strptime(str(row.datetime), '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d %H:%M:00')
                    quantity = row.fee_quantity

                    # Check whether we have already cached the GBP rate for the asset and datetime in question
                    if asset in cached_rates.keys():
                        if not cached_rates[asset].get(datetime):
                            try:
                                c = BinanceConvertToGBP(asset, datetime, quantity)
                                gbp_value = c.convert_to_gbp()
                            except IndexError:
                                c = CoinAPIConvertToGBP(asset, datetime, quantity)
                                gbp_value = c.convert_to_gbp()
                            fee_gbp.append(gbp_value)
                            if float(quantity):
                                rate_gbp = gbp_value / float(quantity)
                                cached_rates[asset][datetime] = rate_gbp
                            count_api += 1
                        else:
                            rate_gbp = cached_rates[asset].get(datetime)
                            quantity_gbp = rate_gbp * float(quantity)
                            fee_gbp.append(quantity_gbp)
                            count_cache += 1
                    else:
                        try:
                            c = BinanceConvertToGBP(asset, datetime, quantity)
                            gbp_value = c.convert_to_gbp()
                        except IndexError:
                            c = CoinAPIConvertToGBP(asset, datetime, quantity)
                            gbp_value = c.convert_to_gbp()
                        fee_gbp.append(gbp_value)
                        if float(quantity):
                            rate_gbp = gbp_value / float(quantity)
                            cached_rates[asset] = {}
                            cached_rates[asset][datetime] = rate_gbp
                        count_api += 1
            else:
                fee_gbp.append(None)

        print(f'Count API:\t {count_api}')
        print(f'Count cache:\t {count_cache}')

        # Save cached_rates back to json file for quicker conversions on next run
        with open('data/cached_gbp_rates.json', 'w', encoding='utf-8') as f:
            json.dump(cached_rates, f, ensure_ascii=False, indent=4)

        df_transactions['final_asset_gbp'] = final_asset_gbp
        df_transactions['fee_gbp'] = fee_gbp

        return df_transactions

    def get_csv(self):
        return pd.read_csv(self.path+os.listdir(self.path)[-1])

    @staticmethod
    def create_transaction_dataframe(row):
        if row.TYPE == 'deposit':
            tx = Transaction()

            tx.transaction['asset'] = row.INCURRENCY
            tx.transaction['action'] = 'deposit_crypto'
            tx.transaction['disposal'] = False
            tx.transaction['datetime'] = dt.strptime(row.DATE.split(' (')[0], '%a %b %d %Y %H:%M:%S GMT%z').strftime('%Y-%m-%d %H:%M:%S')
            tx.transaction['initial_asset_quantity'] = round(row.INAMOUNT, 8)
            tx.transaction['initial_asset_currency'] = row.INCURRENCY
            tx.transaction['initial_asset_location'] = None
            tx.transaction['initial_asset_address'] = None
            tx.transaction['price'] = None
            tx.transaction['final_asset_quantity'] = round(row.INAMOUNT, 8)
            tx.transaction['final_asset_currency'] = row.INCURRENCY
            tx.transaction['final_asset_gbp'] = None
            tx.transaction['final_asset_location'] = 'Exodus'
            tx.transaction['final_asset_address'] = None
            tx.transaction['fee_type'] = None
            tx.transaction['fee_quantity'] = None
            tx.transaction['fee_currency'] = None
            tx.transaction['fee_gbp'] = None
            tx.transaction['source_transaction_id'] = row.INTXID
            tx.transaction['source_trade_id'] = None

            return pd.DataFrame(tx.transaction, index=[0])

        elif row.TYPE == 'withdrawal':
            tx = Transaction()

            tx.transaction['asset'] = row.OUTCURRENCY
            tx.transaction['action'] = 'withdraw_crypto'
            tx.transaction['disposal'] = False
            tx.transaction['datetime'] = dt.strptime(row.DATE.split(' (')[0], '%a %b %d %Y %H:%M:%S GMT%z').strftime(
                '%Y-%m-%d %H:%M:%S')
            tx.transaction['initial_asset_quantity'] = round(abs(row.OUTAMOUNT), 8)
            tx.transaction['initial_asset_currency'] = row.OUTCURRENCY
            tx.transaction['initial_asset_location'] = 'Exodus'
            tx.transaction['initial_asset_address'] = None
            tx.transaction['price'] = None
            tx.transaction['final_asset_quantity'] = round(abs(row.OUTAMOUNT), 8)
            tx.transaction['final_asset_currency'] = row.OUTCURRENCY
            tx.transaction['final_asset_gbp'] = None
            tx.transaction['final_asset_location'] = None
            tx.transaction['final_asset_address'] = None
            tx.transaction['fee_type'] = 'withdrawal'
            tx.transaction['fee_quantity'] = round(abs(row.FEEAMOUNT), 8)
            tx.transaction['fee_currency'] = row.FEECURRENCY
            tx.transaction['fee_gbp'] = None
            tx.transaction['source_transaction_id'] = row.OUTTXID
            tx.transaction['source_trade_id'] = None

            return pd.DataFrame(tx.transaction, index=[0])

        elif row.TYPE == 'exchange':
            tx_buy = Transaction()

            tx_buy.transaction['asset'] = row.INCURRENCY
            tx_buy.transaction['action'] = 'exchange_crypto_for_crypto'
            tx_buy.transaction['disposal'] = False
            tx_buy.transaction['datetime'] = dt.strptime(row.DATE.split(' (')[0], '%a %b %d %Y %H:%M:%S GMT%z').strftime(
                '%Y-%m-%d %H:%M:%S')
            tx_buy.transaction['initial_asset_quantity'] = round(abs(row.OUTAMOUNT), 8)
            tx_buy.transaction['initial_asset_currency'] = row.OUTCURRENCY
            tx_buy.transaction['initial_asset_location'] = 'Exodus'
            tx_buy.transaction['initial_asset_address'] = None
            tx_buy.transaction['price'] = round(row.INAMOUNT / abs(row.OUTAMOUNT), 8)
            tx_buy.transaction['final_asset_quantity'] = round(abs(row.INAMOUNT), 8)
            tx_buy.transaction['final_asset_currency'] = row.INCURRENCY
            tx_buy.transaction['final_asset_gbp'] = None
            tx_buy.transaction['final_asset_location'] = 'Exodus'
            tx_buy.transaction['final_asset_address'] = None
            tx_buy.transaction['fee_type'] = 'exchange'
            tx_buy.transaction['fee_quantity'] = round(abs(row.FEEAMOUNT), 8)
            tx_buy.transaction['fee_currency'] = row.FEECURRENCY
            tx_buy.transaction['fee_gbp'] = None
            tx_buy.transaction['source_transaction_id'] = row.INTXID
            tx_buy.transaction['source_trade_id'] = None

            df_buy = pd.DataFrame(tx_buy.transaction, index=[0])

            tx_sell = Transaction()

            tx_sell.transaction['asset'] = row.OUTCURRENCY
            tx_sell.transaction['action'] = 'exchange_crypto_for_crypto'
            tx_sell.transaction['disposal'] = True
            tx_sell.transaction['datetime'] = dt.strptime(row.DATE.split(' (')[0],
                                                         '%a %b %d %Y %H:%M:%S GMT%z').strftime(
                '%Y-%m-%d %H:%M:%S')
            tx_sell.transaction['initial_asset_quantity'] = round(abs(row.OUTAMOUNT), 8)
            tx_sell.transaction['initial_asset_currency'] = row.OUTCURRENCY
            tx_sell.transaction['initial_asset_location'] = 'Exodus'
            tx_sell.transaction['initial_asset_address'] = None
            tx_sell.transaction['price'] = round(row.INAMOUNT / abs(row.OUTAMOUNT), 8)
            tx_sell.transaction['final_asset_quantity'] = round(abs(row.INAMOUNT), 8)
            tx_sell.transaction['final_asset_currency'] = row.INCURRENCY
            tx_sell.transaction['final_asset_gbp'] = None
            tx_sell.transaction['final_asset_location'] = 'Exodus'
            tx_sell.transaction['final_asset_address'] = None
            tx_sell.transaction['fee_type'] = 'exchange'
            tx_sell.transaction['fee_quantity'] = round(abs(row.FEEAMOUNT), 8)
            tx_sell.transaction['fee_currency'] = row.FEECURRENCY
            tx_sell.transaction['fee_gbp'] = None
            tx_sell.transaction['source_transaction_id'] = row.OUTTXID
            tx_sell.transaction['source_trade_id'] = None

            df_sell = pd.DataFrame(tx_sell.transaction, index=[0])

            return pd.concat([df_buy, df_sell])

        else:
            print(f'New Exodus transaction type: {row.TYPE}')
            print('Please write code to handle this type of transaction')


if __name__ == '__main__':
    x = Exodus()
    x.get_exodus_transactions()
