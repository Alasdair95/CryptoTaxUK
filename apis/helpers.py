import os
import requests
from datetime import datetime, timedelta
import mt4_hst
from io import BytesIO
import pandas as pd
from zipfile import ZipFile

from apis.authentication import CoinbaseProAuth


class Transaction:
    def __init__(self):
        self.transaction = {
            'asset': None,  # Which crypto
            'action': None,  # exchange_fiat_for_crypto, exchange_crypto_for_crypto, exchange_crypto_for_fiat, etc...
            'type': None,  # How Coinbase categorise the action
            'disposal': None,  # Bool - is the action considered a disposal by HMRC
            'datetime': None,  # Datetime of the action
            'initial_asset_quantity': None,  # Quantity of the initial asset in the action
            'initial_asset_currency': None,  # What asset does the action begin with
            'initial_asset_location': None,  # Where is the initial asset? wallet, exchange?
            'initial_asset_address': None,  # Wallet address or exchange wallet id
            'price': None,  # The exchange price if action is exchanging
            'final_asset_quantity': None,  # Quantity of the final asset in the action
            'final_asset_currency': None,  # What asset does the action end with
            'final_asset_gbp': None,  # The final GBP value at the time of the action
            'final_asset_location': None,  # Where is the final asset? wallet, exchange?
            'final_asset_address': None,  # Wallet address or exchange wallet id
            'fee_type': None,  # Exchange fee or transfer fee
            'fee_quantity': None,  # How much is the fee in the issued fee currency
            'fee_currency': None,  # The fee currency
            'fee_gbp': None,  # GBP value of the fee at the time of the action
            'source_transaction_id': None,  # Transaction id from the exchange/wallet where transaction occurred
            'source_trade_id': None  # Additional id field from the exchange/wallet to help match exchanges
        }


class CoinbaseConvertToGBP:
    def __init__(self, asset, dt, quantity):
        self.asset = asset
        self.dt = dt
        self.quantity = float(quantity)
        self.api_key = os.environ.get('COINBASE_PRO_API_KEY')
        self.api_secret = os.environ.get('COINBASE_PRO_API_SECRET')
        self.passphrase = os.environ.get('COINBASE_PRO_API_PASSPHRASE')
        self.base_url = 'https://api.pro.coinbase.com/'
        self.forex_downloads = 'data/forex/'

    def convert_to_gbp(self):
        if not os.path.exists(f'{self.forex_downloads}'):
            os.makedirs(f'{self.forex_downloads}')

        if self.asset == 'GBP':
            pass
        # elif self.asset == 'EUR':
        #     return round(self.quantity * self.get_historical_fiat_gbp_price(base='EUR'), 2)
        elif self.asset == 'BTC':
            quantity_usd = self.quantity * self.get_historical_btc_usd_price()
            return quantity_usd * self.get_historical_fiat_gbp_price()
        elif self.asset in ['EUR', 'USD']:
            return self.quantity * self.get_historical_fiat_gbp_price(base=self.asset)
        else:
            quantity_btc = round(self.quantity * self.get_historical_crypto_btc_price(), 8)
            # Must first convert to USD because most exchanges only have recent GBP prices but will have historical USD
            quantity_usd = quantity_btc * self.get_historical_btc_usd_price()
            return quantity_usd * self.get_historical_fiat_gbp_price()

    def get_historical_crypto_btc_price(self):
        start = self.dt
        end = (datetime.strptime(self.dt, '%Y-%m-%d %H:%M:00') + timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:00')

        path = self.base_url + f'products/{self.asset}-BTC/candles'
        params = {
            'start': start,
            'end': end,
            'granularity': 60
        }
        auth = CoinbaseProAuth(self.api_key, self.api_secret, self.passphrase)
        r = requests.get(path, auth=auth, params=params).json()

        return r[0][4]

    def get_historical_btc_usd_price(self):
        path = self.base_url + f'products/BTC-USD/candles'
        params = {
            'start': f'{self.dt}',
            'end': f'{self.dt}',
            'granularity': 60
        }
        auth = CoinbaseProAuth(self.api_key, self.api_secret, self.passphrase)
        r = requests.get(path, auth=auth, params=params).json()

        return r[0][4]

    def get_historical_fiat_gbp_price(self, base='USD'):
        if base == 'USDT':
            base = 'USD'

        # These datasets appear to be updated weekly
        download_urls = {
            'EUR': (0, 'https://tools.fxdd.com/tools/M1Data/EURGBP.zip'),
            'USD': (1, 'https://tools.fxdd.com/tools/M1Data/GBPUSD.zip')
        }

        if download_urls.get(base)[0]:
            file = 'GBP' + base
        else:
            file = base + 'GBP'

        # Check whether or not the csv already exists
        if file + '.csv' in os.listdir(self.forex_downloads):
            df = pd.read_csv(self.forex_downloads + file + '.csv')
        else:
            # Check whether the hst download already exists
            if file + '.hst' not in os.listdir(self.forex_downloads):
                r = requests.get(download_urls.get(base)[1])
                z = ZipFile(BytesIO(r.content))
                z.extractall(self.forex_downloads)

            df = mt4_hst.read_hst(self.forex_downloads + file + '.hst')

            # Get latest date in the dataframe
            if datetime.strptime(self.dt, '%Y-%m-%d %H:%M:%S') > df.tail(2)['time'].tolist()[0]:
                r = requests.get(download_urls.get(base)[1])
                z = ZipFile(BytesIO(r.content))
                z.extractall(self.forex_downloads)

            df = mt4_hst.read_hst(self.forex_downloads + file + '.hst')

            # Drop columns we don't need
            df.drop(['open', 'high', 'low', 'volume'], axis=1, inplace=True)

            start_idx = df.loc[df['time'] == '2017-11-01 00:00:00'].index.values[0]

            df = df.iloc[start_idx:]

            df['time'] = pd.to_datetime(df['time'])
            df.set_index('time', inplace=True)
            idx = pd.date_range(df.index.min(), df.index.max(), freq='1Min')
            df = df.reindex(idx)
            df.fillna(method='ffill', inplace=True)
            df.reset_index(inplace=True)

            df.to_csv(self.forex_downloads + file + '.csv', index=False)

        try:
            if download_urls.get(base)[0]:
                return 1 / df.loc[df['index'] == self.dt]['close'].tolist()[0]
            else:
                return df.loc[df['index'] == self.dt]['close'].tolist()[0]
        except IndexError:  # Most recent exchange rates have not yet been added to the data source
            url = f'https://api.ratesapi.io/api/{self.dt.split(" ")[0]}?base={base}&symbols={base},GBP'
            r = requests.get(url)
            if r.status_code == 200:
                return r.json()['rates']['GBP']
            else:
                url = f'https://api.ratesapi.io/api/{self.dt.split(" ")[0]}?base=USD&symbols={base},GBP'
                r = requests.get(url)
                if r.status_code == 200:
                    return r.json()['rates']['GBP'] / r.json()['rates'][base]


class BinanceConvertToGBP:
    def __init__(self, asset, dt, quantity):
        self.asset = asset
        self.dt = dt
        self.quantity = float(quantity)
        self.base_url = 'https://api.binance.com'
        self.forex_downloads = 'data/forex/'
        self.rates_api_access_key = os.environ.get('RATES_API_ACCESS_KEY')

    def convert_to_gbp(self):
        if not os.path.exists(f'{self.forex_downloads}'):
            os.makedirs(f'{self.forex_downloads}')

        if self.asset == 'GBP':
            pass
        elif self.asset == 'BTC':
            quantity_usd = self.quantity * self.get_historical_btc_usd_price()
            quantity_gbp = quantity_usd * self.get_historical_fiat_gbp_price()
            return quantity_gbp
        elif self.asset in ['EUR', 'USD', 'USDT']:
            quantity_gbp = self.quantity * self.get_historical_fiat_gbp_price(base=self.asset)
            return quantity_gbp
        else:
            quantity_btc = round(self.quantity * self.get_historical_crypto_btc_price(), 8)
            # Must first convert to USD because most exchanges only have recent GBP prices but will have historical USD
            quantity_usd = quantity_btc * self.get_historical_btc_usd_price()
            quantity_gbp = quantity_usd * self.get_historical_fiat_gbp_price()
            return quantity_gbp

    def get_historical_crypto_btc_price(self):
        path = '/api/v3/klines'

        start_timestamp = int(datetime.timestamp(datetime.strptime(self.dt, '%Y-%m-%d %H:%M:00')))
        end_timestamp = int(datetime.timestamp((datetime.strptime(self.dt, '%Y-%m-%d %H:%M:00')) + timedelta(minutes=1000)))

        params = {
            'symbol': f'{self.asset}BTC',
            'interval': '1m',
            'startTime': start_timestamp*1000,
            'endTime': end_timestamp*1000,
            'limit': 1000
        }

        r = requests.get(self.base_url + path, params=params).json()

        return float(r[0][4])

    def get_historical_btc_usd_price(self):
        path = '/api/v3/klines'

        start_timestamp = int(datetime.timestamp(datetime.strptime(self.dt, '%Y-%m-%d %H:%M:00')))
        end_timestamp = int(
            datetime.timestamp((datetime.strptime(self.dt, '%Y-%m-%d %H:%M:00')) + timedelta(minutes=1000)))

        params = {
            'symbol': 'BTCUSDT',
            'interval': '1m',
            'startTime': start_timestamp * 1000,
            'endTime': end_timestamp * 1000,
            'limit': 1000
        }

        r = requests.get(self.base_url + path, params=params).json()

        return float(r[0][4])

    def get_historical_fiat_gbp_price(self, base='USD'):
        if base == 'USDT':
            base = 'USD'

        # These datasets appear to be updated weekly
        download_urls = {
            'EUR': (0, 'https://tools.fxdd.com/tools/M1Data/EURGBP.zip'),
            'USD': (1, 'https://tools.fxdd.com/tools/M1Data/GBPUSD.zip')
        }

        if download_urls.get(base)[0]:
            file = 'GBP' + base
        else:
            file = base + 'GBP'

        # Check whether or not the csv already exists
        if file + '.csv' in os.listdir(self.forex_downloads):
            df = pd.read_csv(self.forex_downloads + file + '.csv')
        else:
            # Check whether the hst download already exists
            if file + '.hst' not in os.listdir(self.forex_downloads):
                r = requests.get(download_urls.get(base)[1])
                z = ZipFile(BytesIO(r.content))
                z.extractall(self.forex_downloads)

            df = mt4_hst.read_hst(self.forex_downloads + file + '.hst')

            # Get latest date in the dataframe
            if datetime.strptime(self.dt, '%Y-%m-%d %H:%M:%S') > df.tail(2)['time'].tolist()[0]:
                r = requests.get(download_urls.get(base)[1])
                z = ZipFile(BytesIO(r.content))
                z.extractall(self.forex_downloads)

            df = mt4_hst.read_hst(self.forex_downloads + file + '.hst')

            # Drop columns we don't need
            df.drop(['open', 'high', 'low', 'volume'], axis=1, inplace=True)

            start_idx = df.loc[df['time'] == '2017-11-01 00:00:00'].index.values[0]

            df = df.iloc[start_idx:]

            df['time'] = pd.to_datetime(df['time'])
            df.set_index('time', inplace=True)
            idx = pd.date_range(df.index.min(), df.index.max(), freq='1Min')
            df = df.reindex(idx)
            df.fillna(method='ffill', inplace=True)
            df.reset_index(inplace=True)

            df.to_csv(self.forex_downloads + file + '.csv', index=False)

        try:
            if download_urls.get(base)[0]:
                return 1 / df.loc[df['index'] == self.dt]['close'].tolist()[0]
            else:
                return df.loc[df['index'] == self.dt]['close'].tolist()[0]
        except IndexError:  # Most recent exchange rates have not yet been added to the data source
            url = f'http://api.exchangeratesapi.io/v1/{self.dt.split(" ")[0]}?symbols={base},GBP&access_key={self.rates_api_access_key}'
            r = requests.get(url)
            if r.status_code == 200:
                return r.json()['rates']['GBP']
            else:
                url = f'http://api.exchangeratesapi.io/v1/{self.dt.split(" ")[0]}?symbols={base},GBP&access_key={self.rates_api_access_key}'
                r = requests.get(url)
                if r.status_code == 200:
                    return r.json()['rates']['GBP']


class CoinAPIConvertToGBP:
    def __init__(self, asset, dt, quantity):
        self.asset = asset
        self.dt = dt
        self.quantity = float(quantity)
        self.coin_api_key = os.environ.get('COIN_API_KEY')
        self.base_url = 'https://rest.coinapi.io'
        self.forex_downloads = 'data/forex/'

    def convert_to_gbp(self):
        if not os.path.exists(f'{self.forex_downloads}'):
            os.makedirs(f'{self.forex_downloads}')

        quantity_usd = self.quantity * self.get_historical_btc_usd_price()
        quantity_gbp = quantity_usd * self.get_historical_fiat_gbp_price()

        return quantity_gbp

    def get_historical_btc_usd_price(self):
        headers = {
            'X-CoinAPI-Key': self.coin_api_key
        }

        path = '/v1/symbols?filter_symbol_id=KRAKEN_SPOT_BCH_USD'

        symbol_id = f'KRAKEN_SPOT_{self.asset}_USD'
        path = f'/v1/ohlcv/{symbol_id}/history?'

        params = {
            'period_id': '1MIN',
            'time_start': self.dt.replace(' ', 'T')
        }

        r = requests.get(self.base_url + path, headers=headers, params=params).json()

        return r[0]['price_close']

    def get_historical_fiat_gbp_price(self, base='USD'):
        if base == 'USDT':
            base = 'USD'

        # These datasets appear to be updated weekly
        download_urls = {
            'EUR': (0, 'https://tools.fxdd.com/tools/M1Data/EURGBP.zip'),
            'USD': (1, 'https://tools.fxdd.com/tools/M1Data/GBPUSD.zip')
        }

        if download_urls.get(base)[0]:
            file = 'GBP' + base
        else:
            file = base + 'GBP'

        # Check whether or not the csv already exists
        if file + '.csv' in os.listdir(self.forex_downloads):
            df = pd.read_csv(self.forex_downloads + file + '.csv')
        else:
            # Check whether the hst download already exists
            if file + '.hst' not in os.listdir(self.forex_downloads):
                r = requests.get(download_urls.get(base)[1])
                z = ZipFile(BytesIO(r.content))
                z.extractall(self.forex_downloads)

            df = mt4_hst.read_hst(self.forex_downloads + file + '.hst')

            # Get latest date in the dataframe
            if datetime.strptime(self.dt, '%Y-%m-%d %H:%M:%S') > df.tail(2)['time'].tolist()[0]:
                r = requests.get(download_urls.get(base)[1])
                z = ZipFile(BytesIO(r.content))
                z.extractall(self.forex_downloads)

            df = mt4_hst.read_hst(self.forex_downloads + file + '.hst')

            # Drop columns we don't need
            df.drop(['open', 'high', 'low', 'volume'], axis=1, inplace=True)

            start_idx = df.loc[df['time'] == '2017-11-01 00:00:00'].index.values[0]

            df = df.iloc[start_idx:]

            df['time'] = pd.to_datetime(df['time'])
            df.set_index('time', inplace=True)
            idx = pd.date_range(df.index.min(), df.index.max(), freq='1Min')
            df = df.reindex(idx)
            df.fillna(method='ffill', inplace=True)
            df.reset_index(inplace=True)

            df.to_csv(self.forex_downloads + file + '.csv', index=False)

        try:
            if download_urls.get(base)[0]:
                return 1 / df.loc[df['index'] == self.dt]['close'].tolist()[0]
            else:
                return df.loc[df['index'] == self.dt]['close'].tolist()[0]
        except IndexError:  # Most recent exchange rates have not yet been added to the data source
            url = f'https://api.ratesapi.io/api/{self.dt.split(" ")[0]}?base={base}&symbols={base},GBP'
            r = requests.get(url)
            if r.status_code == 200:
                return r.json()['rates']['GBP']
            else:
                url = f'https://api.ratesapi.io/api/{self.dt.split(" ")[0]}?base=USD&symbols={base},GBP'
                r = requests.get(url)
                if r.status_code == 200:
                    return r.json()['rates']['GBP'] / r.json()['rates'][base]


if __name__ == '__main__':
    x = BinanceConvertToGBP('USD', '2021-07-20 19:45:00', 49.7)
    # x.convert_to_gbp()
    x.get_historical_fiat_gbp_price()
