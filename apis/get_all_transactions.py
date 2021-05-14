import os
import pandas as pd

from apis.exchanges.binance import Binance
from apis.exchanges.coinbase import Coinbase
from apis.exchanges.coinbase_pro import CoinbasePro

from apis.wallets.exodus import Exodus


class GetAllTransactions:
    def __init__(self):
        self.save_path = 'data/'

    def get_all_transactions(self):
        # TODO: Add print statements for progress updates throughout all pipelines

        # Create transaction CSVs from exchanges
        self.create_exchange_transactions()

        # Create transaction CSVs from wallets
        self.create_wallet_transactions()

        transaction_dfs = []

        for i in os.listdir(self.save_path):
            transaction_dfs.append(pd.read_csv(self.save_path+i))

        all_transactions = pd.concat(transaction_dfs)

        # Remove duplicate deposit_crypto transactions
        deposit_external = all_transactions.loc[(all_transactions['action'] == 'deposit_crypto') & (all_transactions['type'] == 'external')]
        deposit_send = all_transactions.loc[(all_transactions['action'] == 'deposit_crypto') & (all_transactions['type'] == 'send')]

        to_drop = pd.merge(deposit_external, deposit_send, how='inner', left_on=['asset', 'final_asset_quantity'], right_on=['asset', 'final_asset_quantity'])['source_transaction_id_y'].tolist()

        all_transactions = all_transactions[~all_transactions.source_transaction_id.isin(to_drop)]

        asset_transaction_dfs = {}
        for asset in all_transactions['asset'].unique():
            asset_transaction_dfs[asset] = all_transactions.loc[all_transactions['asset'] == asset].sort_values(by='datetime')

        for asset, df in asset_transaction_dfs.items():
            deposits = df.loc[df['action'] == 'deposit_crypto']
            withdrawals = df.loc[df['action'] == 'withdraw_crypto']

            matches = pd.merge(deposits, withdrawals, how='inner',
                               left_on=['asset', 'initial_asset_quantity'],
                               right_on=['asset', 'final_asset_quantity'])

            matches = matches.loc[matches['final_asset_location_x'] != matches['initial_asset_location_y']]

            # Create the deposits rows
            d = matches.drop(['action_y', 'type_y', 'disposal_y', 'datetime_y', 'initial_asset_quantity_y',
                              'initial_asset_currency_y', 'initial_asset_location_x', 'initial_asset_address_y',
                              'price_y', 'final_asset_quantity_y', 'final_asset_currency_y', 'final_asset_gbp_y',
                              'final_asset_location_y', 'final_asset_address_y', 'fee_type_y',	'fee_quantity_y',
                              'fee_currency_y',	 'fee_gbp_y', 'source_transaction_id_y', 'source_trade_id_y'], axis=1)

            d_rename_map = {i: '_'.join(i.split('_')[:-1]) for i in d.columns if '_' in i}

            d.rename(columns=d_rename_map, inplace=True)

            w = matches.drop(['action_x', 'type_x', 'disposal_x', 'datetime_x', 'initial_asset_quantity_x',
                              'initial_asset_currency_x', 'initial_asset_location_x', 'initial_asset_address_x',
                              'price_x', 'final_asset_quantity_x', 'final_asset_currency_x', 'final_asset_gbp_x',
                              'final_asset_location_y', 'final_asset_address_x', 'fee_type_x', 'fee_quantity_x',
                              'fee_currency_x', 'fee_gbp_x', 'source_transaction_id_x', 'source_trade_id_x'], axis=1)

            w_rename_map = {i: '_'.join(i.split('_')[:-1]) for i in w.columns if '_' in i}

            w.rename(columns=w_rename_map, inplace=True)

            rows_to_drop = d['source_transaction_id'].tolist() + w['source_transaction_id'].tolist()

            df = df[~df.source_transaction_id.isin(rows_to_drop)]

            df = pd.concat([df, d, w]).sort_values(by='datetime')

            # Put columns in correct order again

            df = df[['asset', 'action', 'type', 'disposal', 'datetime', 'initial_asset_quantity',
                     'initial_asset_currency', 'initial_asset_location', 'initial_asset_address', 'price',
                     'final_asset_quantity', 'final_asset_currency', 'final_asset_gbp', 'final_asset_location',
                     'final_asset_address', 'fee_type', 'fee_quantity', 'fee_currency', 'fee_gbp',
                     'source_transaction_id', 'source_trade_id']]

            # Overwrite the dataframe in the dict
            asset_transaction_dfs[asset] = df

        return asset_transaction_dfs

    def create_exchange_transactions(self):
        # Get Binance transactions
        print('Getting Binance transactions...')
        binance = Binance()
        binance_transactions = binance.get_binance_transactions()
        binance_transactions = pd.to_csv(f'{self.save_path}binance.csv', index=False)
        print('Got Binance transactions!\n')

        # Get Coinbase transactions
        print('Getting Coinbase transactions...')
        coinbase = Coinbase()
        coinbase_transactions = coinbase.get_coinbase_transactions()
        coinbase_transactions = pd.to_csv(f'{self.save_path}coinbase.csv', index=False)
        print('Got Coinbase transactions!\n')

        # Get Coinbase Pro transactions
        print('Getting Coinbase Pro transactions...')
        coinbase_pro = CoinbasePro()
        coinbase_pro_transactions = coinbase_pro.get_coinbase_pro_transactions()
        coinbase_pro = pd.to_csv(f'{self.save_path}coinbase_pro.csv', index=False)
        print('Got Coinbase Pro transactions!\n')

    def create_wallet_transactions(self):
        # Get Exodus transactions
        print('Getting Exodus transactions...')
        exodus = Exodus()
        exodus_transactions = exodus.get_exodus_transactions()
        exodus_transactions = pd.to_csv(f'{self.save_path}exodus.csv', index=False)
        print('Got Exodus transactions!\n')


if __name__ == '__main__':
    x = GetAllTransactions()
    x.get_all_transactions()
