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
                         'final_asset_location', 'final_asset_address', 'fee_type', 'fee_quantity', 'fee_gbp',
                         'fee_currency', 'source_transaction_id', 'source_trade_id'], axis=1, inplace=True)

                # Tag same day transactions
                # df will have max one acquisition and max one disposal per day according to the same day rule
                df = TaxCalculations.tag_same_day_transactions(df)

                # Add required columns that will be populated as we loop through df
                df['section_104_pool'] = None
                df['section_104_allowable_cost'] = None
                df['section_104_pool_update'] = None
                # df['same_day_remainder_pool'] = None
                # df['same_day_allowable_cost'] = None
                df['thirty_day_rule_remainder_pool'] = None
                df['thirty_day_rule_allowable_cost'] = None
                df['same_day_profit_or_loss'] = None
                df['thirty_day_rule_profit_or_loss'] = None
                df['section_104_profit_or_loss'] = None

                # Loop through dataframe and perform required calculations
                # Order of calculation priority:
                # - Same day
                # - 30 day rule
                # - Section 104

                # Handle the very first acquisition
                df['section_104_pool'] = round(df.at[0, 'final_asset_quantity'], 8)
                df['section_104_allowable_cost'] = round(df.at[0, 'final_asset_gbp'], 2)

                unmatched_transactions = []
                for row in df.itertuples():
                    if row.same_day:
                        df_tuple = TaxCalculations.same_day_transaction_calculations(row, df)
                        df = df_tuple[0]
                        if df_tuple[1] is not None:
                            unmatched_transactions.append(df_tuple[1])

                df = pd.concat([df, pd.concat(unmatched_transactions)])\
                    .sort_values(by=['day', 'same_day'], ascending=[True, False]).reset_index(drop=True)

                # Update section_104_pool accordingly
                # section_104_pool = df['section_104_pool'].tolist()
                # for i, row in enumerate(df.itertuples()):
                #     if row.section_104_pool_update:
                #         section_104_pool[i:] = [section_104_pool[i] + row.section_104_pool_update]\
                #                                * (len(section_104_pool) - i)

                # Tag 30 day rule transactions
                # If there is an acquisition within 30 days of the most recent disposal the 30 day rule applies
                df = TaxCalculations.tag_thirty_day_rule_transactions(df)

                # Reorder columns
                df = df[['day', 'action_type', 'initial_asset_quantity', 'final_asset_quantity',
                         'final_asset_gbp', 'same_day', 'thirty_day_rule', 'section_104_pool',
                         'section_104_allowable_cost', 'section_104_pool_update', 'thirty_day_rule_remainder_pool',
                         'thirty_day_rule_allowable_cost', 'same_day_profit_or_loss',
                         'thirty_day_rule_profit_or_loss', 'section_104_profit_or_loss']]

                # TODO: It's still all wrong because you're updating the S104 pool straight after the same day calcs...
                #   Need to add a section_104_allowable_cost_update field
                #   Update 30D tag method to ignore SD acquisitions that have been accounted for in SD calculations


                # for row in df.itertuples():
                #     if row.same_day:
                #         df = TaxCalculations.same_day_transaction_calculations(row, df)
                #     if row.thirty_day_rule:
                #         df = TaxCalculations.thirty_day_rule_transaction_calculations(row, df)
                #
                #     df = TaxCalculations.section_104_transaction_calculations(row, df)

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

        df['thirty_day_rule'] = thirty_day_rule_transactions

        return df

    @staticmethod
    def same_day_transaction_calculations(row, df):
        if row.action_type == 'acquisition':
            df_day = df.loc[df['day'] == row.day]
            acquisition_quantity = df_day.loc[df_day['action_type'] == 'acquisition']['final_asset_quantity'].tolist()[0]
            acquisition_ac = df_day.loc[df_day['action_type'] == 'acquisition']['final_asset_gbp'].tolist()[0]
            disposal_quantity = df_day.loc[df_day['action_type'] == 'disposal']['initial_asset_quantity'].tolist()[0]
            disposal_ac = df_day.loc[df_day['action_type'] == 'disposal']['final_asset_gbp'].tolist()[0]

            new_transaction = None

            if disposal_quantity == acquisition_quantity:
                # Disposal is equal to acquisition so calculation is isolated and Section 104 pool does not change
                # Update acquisition row
                df.at[row.Index, 'section_104_pool'] = df.at[row.Index - 1, 'section_104_pool']
                df.at[row.Index, 'section_104_allowable_cost'] = df.at[row.Index - 1, 'section_104_allowable_cost']
                # Update disposal row
                df.at[row.Index + 1, 'section_104_pool'] = df.at[row.Index - 1, 'section_104_pool']
                df.at[row.Index + 1, 'section_104_allowable_cost'] = df.at[row.Index - 1, 'section_104_allowable_cost']
                df.at[row.Index + 1, 'same_day_profit_or_loss'] = disposal_ac - acquisition_ac
            elif disposal_quantity > acquisition_quantity:
                # Disposal is greater than acquisition so same day gain is only calculated for matched assets
                # Remaining acquisition is held separate to S104 pool in case 30 day rule applies
                less_allowable_costs = acquisition_ac * (disposal_quantity / acquisition_quantity)
                gain = round(disposal_ac - less_allowable_costs, 2)
                remaining_disposal = round(disposal_quantity - acquisition_quantity, 8)

                # Update acquisition row (could be overwritten when applying 30 day rule)
                df.at[row.Index, 'section_104_pool'] = df.at[row.Index - 1, 'section_104_pool']
                df.at[row.Index, 'section_104_allowable_cost'] = df.at[row.Index - 1, 'section_104_allowable_cost']
                # Update disposal row (could be overwritten when applying 30 day rule)
                df.at[row.Index + 1, 'section_104_pool'] = df.at[row.Index - 1, 'section_104_pool']
                df.at[row.Index + 1, 'section_104_allowable_cost'] = df.at[row.Index - 1, 'section_104_allowable_cost']
                # df.at[row.Index + 1, 'same_day_remainder_pool'] = remaining_disposal
                # df.at[row.Index + 1, 'same_day_profit_or_loss'] = gain

                # Add excess disposal as a new disposal record that will be handled by the Section 104 method
                d = {
                    'day': row.day,
                    'action_type': 'disposal',
                    'initial_asset_quantity': remaining_disposal,
                    'final_asset_quantity': None,
                    'final_asset_gbp': round(acquisition_ac * (remaining_disposal / acquisition_quantity), 2),
                    'same_day': False,
                    'thirty_day_rule': False,
                    'section_104_pool': round(df.at[row.Index - 1, 'section_104_pool'], 8),
                    'section_104_allowable_cost': round(df.at[row.Index - 1, 'section_104_allowable_cost'] - (acquisition_ac * (remaining_disposal / acquisition_quantity)), 2),
                    'section_104_pool_update': round(-1 * remaining_disposal, 8),
                    'thirty_day_rule_remainder_pool': 0,
                    'thirty_day_rule_allowable_cost': 0,
                    'same_day_profit_or_loss': 0,
                    'thirty_day_rule_profit_or_loss': 0,
                    'section_104_profit_or_loss': 0
                }
                new_transaction = pd.DataFrame(d, index=[0])
            else:
                # Acquisition is greater than disposal so gain is calculated for entire disposal
                # Remaining acquisition is held separate to S104 pool in case 30 day rule applies
                less_allowable_costs = acquisition_ac * (disposal_quantity / acquisition_quantity)
                gain = round(disposal_ac - less_allowable_costs, 2)
                remaining_acquisition = round(acquisition_quantity - disposal_quantity, 8)

                # Update acquisition row (could be overwritten when applying 30 day rule)
                df.at[row.Index, 'section_104_pool'] = df.at[row.Index - 1, 'section_104_pool']
                df.at[row.Index, 'section_104_allowable_cost'] = df.at[row.Index - 1, 'section_104_allowable_cost']
                # df.at[row.Index, 'same_day_remainder_pool'] = remaining_acquisition
                # Update disposal row (could be overwritten when applying 30 day rule)
                df.at[row.Index + 1, 'section_104_pool'] = df.at[row.Index - 1, 'section_104_pool']
                df.at[row.Index + 1, 'section_104_allowable_cost'] = df.at[row.Index - 1, 'section_104_allowable_cost']
                df.at[row.Index + 1, 'same_day_profit_or_loss'] = gain

                # Add excess acquisition as a new acquisition record that will be handled by the Section 104 method
                d = {
                    'day': row.day,
                    'action_type': 'acquisition',
                    'initial_asset_quantity': remaining_acquisition,
                    'final_asset_quantity': None,
                    'final_asset_gbp': None,
                    'same_day': False,
                    'thirty_day_rule': False,
                    'section_104_pool': round(df.at[row.Index - 1, 'section_104_pool'], 8),
                    'section_104_allowable_cost': df.at[row.Index - 1, 'section_104_allowable_cost'],
                    'section_104_pool_update': round(remaining_acquisition, 8),
                    'thirty_day_rule_remainder_pool': 0,
                    'thirty_day_rule_allowable_cost': 0,
                    'same_day_profit_or_loss': 0,
                    'thirty_day_rule_profit_or_loss': 0,
                    'section_104_profit_or_loss': 0
                }
                new_transaction = pd.DataFrame(d, index=[0])
        else:
            new_transaction = None

        return df, new_transaction

    @staticmethod
    def thirty_day_rule_transaction_calculations(row, df):

        # Any remaining assets outside of the Section 104 pool are now added back in and the allowable cost updated


        return df

    @staticmethod
    def section_104_transaction_calculations(row, df):
        if row.action_type == 'acquisition':  # TODO: Ensure this only considers funds not already accounted for with SD or 30D
            # Handle very first acquisition
            if row.Index == 0:
                df.at[row.Index, 'section_104_pool'] = round(row.final_asset_quantity, 8)
                df.at[row.Index, 'section_104_allowable_cost'] = round(row.final_asset_gbp, 2)
            else:
                df.at[row.Index, 'section_104_pool'] = round(df.at[row.Index - 1, 'section_104_pool']\
                                                             + row.final_asset_quantity, 8)
                df.at[row.Index, 'section_104_allowable_cost'] = round(df.at[row.Index - 1, 'section_104_allowable_cost']\
                                                                    + row.final_asset_gbp, 2)
        else:
            less_allowable_costs = df.at[row.Index-1, 'section_104_allowable_cost']\
                                   * (row.initial_asset_quantity / df.at[row.Index - 1, 'section_104_pool'])
            gain = row.final_asset_gbp - less_allowable_costs

            df.at[row.Index, 'section_104_pool'] = round(df.at[row.Index - 1, 'section_104_pool']\
                                                         - row.initial_asset_quantity, 8)
            df.at[row.Index, 'section_104_allowable_cost'] = round(df.at[row.Index - 1, 'section_104_allowable_cost']\
                                                             - less_allowable_costs, 2)
            df.at[row.Index, 'section_104_profit_or_loss'] = round(gain, 2)

        return df


if __name__ == '__main__':
    x = TaxCalculations()
    x.tax_calculations()
