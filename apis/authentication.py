import time
from requests.auth import AuthBase
import hmac
import hashlib


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