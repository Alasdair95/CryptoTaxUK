import os
import re
import requests
import pandas as pd

from apis.authentication import CoinbaseProAuth
from apis.helpers import Transaction


class CoinbasePro:
    def __init__(self):
        self.api_key = os.environ.get('COINBASE_PRO_API_KEY')
        self.api_secret = os.environ.get('COINBASE_PRO_API_SECRET')
        self.passphrase = os.environ.get('COINBASE_PRO_API_PASSPHRASE')
        self.base_url = 'https://api.pro.coinbase.com/'

    def test(self):
            # Set up authentication and make call to the API
            path = self.base_url + 'accounts'
            auth = CoinbaseProAuth(self.api_key, self.api_secret, self.passphrase)
            r = requests.get(path, auth=auth).json()
            print(r)


if __name__ == '__main__':
    x = CoinbasePro()
    x.test()

