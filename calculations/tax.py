import os
import pandas as pd
from datetime import datetime as dt


class TaxCalculations:
    def __init__(self):
        self.asset_paths = {'data/asset_transactions/'+i: False
                            for i in os.listdir('data/asset_transactions/')
                            if i.split('.')[0] not in ['GBP', 'EUR']}

    def tax_calculations(self):
        for asset_path in self.asset_paths:
            if asset_path.split('/')[-1].split('.')[0] == 'BTC':  # TODO: For dev purposes only, remove later
                df = pd.read_csv(asset_path)

                # Tag rows as acquisitions or disposals
                df = TaxCalculations.tag_acquisition_or_disposal(df)

                # Add day field
                df['day'] = df['datetime'].apply(lambda x: x.split(' ')[0])

                # Drop rows not required for tax calculations
                df.drop(['action', 'type', 'disposal', 'datetime', 'initial_asset_currency',
                         'initial_asset_location', 'initial_asset_address', 'price', 'final_asset_currency',
                         'final_asset_location', 'final_asset_address', 'fee_type', 'fee_quantity',
                         'fee_currency', 'source_transaction_id', 'source_trade_id'], axis=1, inplace=True)

                # Tag same day transactions
                # df will have max one acquisition and max one disposal per day according to the same day rule
                df = TaxCalculations.tag_same_day_transactions(df)

                # Tag 30 day rule transactions
                # If there is an acquisition within 30 days of the most recent disposal the 30 day rule applies
                df = TaxCalculations.tag_thirty_day_rule_transactions(df)

                # Loop through dataframe and perform required calculations
                # Order of calculation priority:
                # - Same day
                # - 30 day rule
                # - Section 104
                for row in df.itertuples():
                    if row['same_day']:
                        df = TaxCalculations.same_day_transacation_calculations(row.day, df)
                    elif row['30_day_rule']:
                        df = TaxCalculations.thirty_day_rule_transacation_calculations(row.day, df)
                    else:
                        df = TaxCalculations.section_104_transacation_calculations(row.day, df)


                pass

    @staticmethod
    def tag_acquisition_or_disposal(df):
        action_types = []
        for row in df.itertuples():
            if row.disposal:
                action_type = 'disposal'
            elif row.action in ['exchange_fiat_for_crypto', 'exchange_crypto_for_crypto']:
                action_type = 'acquisition'
            else:
                action_type = None

            action_types.append(action_type)

        df['action_type'] = action_types

        return df.loc[df['action_type'].notnull()]

    @staticmethod
    def tag_same_day_transactions(df):
        df_g = df.groupby(['day', 'action_type'], as_index=False).sum()

        df_gc = df_g.merge(df_g.groupby('day').size().reset_index(name='count'), left_on='day', right_on='day')

        df_gc['same_day'] = df_gc['count'].apply(lambda x: True if x > 1 else False)

        df_gc.drop('count', axis=1, inplace=True)

        return df_gc

    @staticmethod
    def tag_thirty_day_rule_transactions(df):
        most_recent_disposal = dt.strptime('2000-01-01', '%Y-%m-%d')
        thirty_day_rule_transactions = []
        for row in df.itertuples():
            if row.action_type == 'disposal':
                most_recent_disposal = dt.strptime(row.day, '%Y-%m-%d')
                thirty_day_rule_transactions.append(False)
            else:
                if (dt.strptime(row.day, '%Y-%m-%d') - most_recent_disposal).days <= 30:
                    thirty_day_rule_transactions.append(True)
                else:
                    thirty_day_rule_transactions.append(False)

        df['30_day_rule'] = thirty_day_rule_transactions

        return df

    @staticmethod
    def same_day_transacation_calculations(day, df):

        return df

    @staticmethod
    def thirty_day_rule_transacation_calculations(day, df):

        return df

    @staticmethod
    def section_104_transacation_calculations(day, df):

        return df




if __name__ == '__main__':
    x = TaxCalculations()
    x.tax_calculations()
