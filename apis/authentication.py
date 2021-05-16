import time
from requests.auth import AuthBase
import hmac
import hashlib
import base64
from urllib.parse import urlencode


class CoinbaseAuth(AuthBase):
    def __init__(self, api_key, api_secret):
        self.api_key = api_key
        self.api_secret = api_secret

    def __call__(self, request):
        timestamp = str(int(time.time()))
        message = timestamp + request.method + request.path_url + (request.body or '')

        if not isinstance(message, bytes):
            message = message.encode()
        if not isinstance(self.api_secret, bytes):
            self.api_secret = self.api_secret.encode()

        signature = hmac.new(self.api_secret, message, hashlib.sha256).hexdigest()

        request.headers.update({
            'CB-ACCESS-SIGN': signature,
            'CB-ACCESS-TIMESTAMP': timestamp,
            'CB-ACCESS-KEY': self.api_key,
        })

        return request


class CoinbaseProAuth(AuthBase):
    def __init__(self, api_key, api_secret, passphrase):
        self.api_key = api_key
        self.api_secret = api_secret
        self.passphrase = passphrase

    def __call__(self, request):
        timestamp = str(time.time())
        message = timestamp + request.method + request.path_url + (request.body or '')

        message = message.encode('ascii')
        hmac_key = base64.b64decode(self.api_secret)
        signature = hmac.new(hmac_key, message, hashlib.sha256)
        signature_b64 = base64.b64encode(signature.digest()).decode('utf-8')
        request.headers.update({
            'Content-Type': 'Application/JSON',
            'CB-ACCESS-SIGN': signature_b64,
            'CB-ACCESS-TIMESTAMP': timestamp,
            'CB-ACCESS-KEY': self.api_key,
            'CB-ACCESS-PASSPHRASE': self.passphrase
        })
        return request


class BinanceAuth:
    def __init__(self, api_key, api_secret):
        self.api_key = api_key
        self.api_secret = api_secret

    def get_request_headers(self):
        headers = {
            'X-MBX-APIKEY': self.api_key
        }

        return headers

    def get_request_signature(self, params):
        query_string = urlencode(params)
        signature = hmac.new(self.api_secret.encode('utf-8'), query_string.encode('utf-8'),
                             hashlib.sha256).hexdigest()

        return signature
