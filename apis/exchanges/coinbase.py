import os
import requests

from apis.authentication import CoinbaseAuth


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
        all_transactions = {}
        for wallet in wallets_with_transactions:
            all_transactions[wallet['asset']] = self.get_transactions(wallet['id'])

        # Convert all transactions into pandas dataframes

        return None

    @staticmethod
    def pagination(response):
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


if __name__ == '__main__':
    x = Coinbase()
    x.get_coinbase_transactions()
    # x.get_transactions('f7b61c43-93c9-5b94-b4b0-719e09ad9888')
