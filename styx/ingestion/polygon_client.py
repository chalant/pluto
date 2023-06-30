import typing

import requests
from requests import get as requests_get

import pandas as pd
from pandas import Timestamp

from polygon.rest import RESTClient

# todo: there is a header response that contains the number of remaining calls.
# todo: there is a limit of 2000 requests minute and 100000 calls per day...
# todo: there is a cost for each api call... ex: intraday costs 5 etc.


_SECOND = 1

# todo: check header for remaining requests etc.

# todo: raise an error if this fails... error 404, 402
#  we could skip error 404 and continue but raise when it's 402
#  we could display a warning for 404


BASE = URL = f'https://api.polygon.io/'


class PolygonClient(object):
    _api_url = 'api.polygon.io'

    def __init__(self, token, num_threads, response_handler, bars_limit=120):

        self._token = token

        self._session = session = requests.Session()
        session.params["apiKey"] = token

        RESTClient()

        self._second_rate_limit = 100

        self._second_calls = [0] * num_threads
        self._bars_limit = bars_limit

        self._response_handler = response_handler

    @property
    def bars_limit(self):
        return self._bars_limit

    def should_wait(self, elapsed: float) -> int:
        if sum(self._second_calls) > self._second_rate_limit and elapsed < _SECOND:
            return (int(_SECOND - elapsed)) + 1
        return 0

    def reset_calls(self, index):
        self._second_calls[index] = 0

    def get_asset_list(self):
        url = f'https://api.polygon.io/v3/reference/tickers?{self._token}'
        resp = self._session.get(url)

    def get_asset_types(self, asset_class=None):
        url = f"https://api.polygon.io/v3/reference/exchanges?asset_class={asset_class}s&apiKey={self._token}"
        resp = self._session.get(url)

    def get_exchanges(self, asset_class=None):
        url = f"https://api.polygon.io/v3/reference/exchanges?asset_class={asset_class}&apiKey={self._token}"
        resp = self._session.get(url)

    def get_daily_historical_bars(
            self,
            index: int,
            symbol: str,
            range_start: pd.Timestamp,
            range_end: pd.Timestamp) -> typing.Tuple[typing.Tuple, typing.List[typing.List]]:

        url = f""
        session = self._session
        api_token = self._token

        # replace "." with "-" in markets
        if symbol.count(".") == 2:
            symbol = symbol.replace(".", "-", 1)

        # todo: format data properly
        # todo: track request costs...

        # todo: return a tuple where the first element is the header
        #  and the second in the list of all the values

        interval = 'day'

        date_to = range_end.strftime("%Y-%m-%d")
        date_from = range_start.strftime("%Y-%m-%d")

        resp = _rest_get(
            session,
            api_token,
            "eod",
            symbol,
            f"&interval={interval}&from={str(date_from)}&to={str(date_to)}",
            fmt="csv")

        for c in resp.text.split("\n")[1::]:
            # todo: convert timestamp
            yield c[0], c[1], c[2], c[3], c[4], c[6]

        self._second_calls[index] += 1

    def get_minute_historical_bars(self, index: int, symbol: str, range_start: pd.Timestamp, range_end: pd.Timestamp):
        multiplier = '1'
        timespan = 'minute'
        from_ = range_start.strftime("%Y-%m-%d")
        to = range_end.strftime("%Y-%m-%d")
        url = f"{self._api_url}/v2/aggs/ticker/{symbol}/range/{multiplier}/{timespan}/{from_}/{to}"
        resp = self._session.get(url)
        self._response_handler.handle_response(resp)
        for c in resp.text.split("\n")[1::]:
            # todo: convert timestamp
            yield c[0], c[1], c[2], c[3], c[4], c[6]
        self._second_calls[index] += 1

    def get_dividends(
            self,
            index,
            symbol,
            from_: pd.Timestamp,
            to: pd.Timestamp) -> requests.Response:

        date_to = to.strftime("%Y-%m-%d")
        date_from = from_.strftime("%Y-%m-%d")
        resp = requests_get(f"{self._api_url}/div/{symbol}?api_token={self._token}&fmt=json&from={date_from}&to{date_to}")
        return resp

    def get_splits(
            self,
            index,
            symbol,
            from_: pd.Timestamp,
            to: pd.Timestamp) -> requests.Response:
        date_to = to.strftime("%Y-%m-%d")
        date_from = from_.strftime("%Y-%m-%d")

        resp = requests_get(f"{self._api_url}/splits/{symbol}?api_token={self._token}&fmt=json&from={date_from}&to{date_to}")

        self._num_calls[index] += 1

        return resp

    def get_exchanges(self) -> requests.Response:
        """Get supported exchanges"""

        return _rest_get(self._token, "exchanges-list")

    def get_exchange_symbols(self, exchange: str = "") -> requests.Response:
        """Get supported exchange symbols"""

        if exchange.strip() == "":
            raise ValueError("endpoint uri is empty!")

        return _rest_get(self._token, "exchange-symbol-list", exchange)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._session.close()


def get_exchanges(api_key) -> requests.Response:
    """Get supported exchanges"""

    return _rest_get(api_key, "exchanges-list")


def get_exchange_symbols(api_key, exchange: str = "") -> requests.Response:
    """Get supported exchange symbols"""

    if exchange.strip() == "":
        raise ValueError("endpoint uri is empty!")

    return _rest_get(api_key, "exchange-symbol-list", exchange)