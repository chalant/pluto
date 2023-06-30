import typing
import math

from datetime import datetime

import requests
from requests import get as requests_get

import pandas as pd
from pandas import Timestamp

_api_url = "https://eodhistoricaldata.com/api"

INTRADAY_CALLS = 5

# todo: there is a header response that contains the number of remaining calls.
# todo: there is a limit of 2000 requests minute and 100000 calls per day...
# todo: there is a cost for each api call... ex: intraday costs 5 etc.


_MINUTE_SECONDS = 3600

# todo: check header for remaining requests etc.

# todo: raise an error if this fails... error 404, 402
#  we could skip error 404 and continue but raise when it's 402
#  we could display a warning for 404


class EODHDUserApiState(object):
    def __init__(self, api_calls):
        self.api_calls = api_calls


def get_api_state(token):
    resp = requests_get(f"https://eodhistoricaldata.com/api/user/?api_token={token}").json()
    # todo: request
    return EODHDUserApiState()


class EODHDClient(object):
    def __init__(self, token, num_threads):

        self._token = token

        self._session = session = requests.Session()

        # retrieve account state.
        resp = session.get(f"https://eodhistoricaldata.com/api/user/?api_token={token}").json()
        total_calls = resp['apiRequests']
        calls_per_thread = math.floor(total_calls / num_threads)

        self._daily_rate_limit = resp['dailyRateLimit'] - total_calls
        self._minute_rate_limit = 1000

        self._num_calls = [calls_per_thread] * num_threads
        self._minute_calls = [0] * num_threads

    @property
    def daily_rate_limit(self):
        return self._daily_rate_limit

    @property
    def minute_rate_limit(self):
        return self._minute_rate_limit

    def should_wait(self, elapsed: float) -> int:
        if sum(self._minute_calls) > self._minute_rate_limit and elapsed < _MINUTE_SECONDS:
            return (int(_MINUTE_SECONDS - elapsed)) + 5

        if sum(self._num_calls) > self._daily_rate_limit:
            next_midnight = pd.Timestamp(
                datetime.combine(datetime.today(), datetime.min.time()), tz='utc') + pd.Timedelta(days=1)
            now = pd.Timestamp.utcnow()

            # wait until next midnight...
            if pd.Timestamp.utcnow() < next_midnight:
                wait = next_midnight - now
                return wait.seconds + _MINUTE_SECONDS

            return 0

        return 0

    def reset_calls(self, index):
        self._minute_calls[index] = 0

    def get_historical_data(
            self,
            index: int,
            symbol: str,
            interval: str,
            range_start: pd.Timestamp,
            range_end: pd.Timestamp) -> typing.Tuple[typing.Tuple, typing.List[typing.List]]:

        session = self._session
        api_token = self._token

        # replace "." with "-" in markets
        if symbol.count(".") == 2:
            symbol = symbol.replace(".", "-", 1)

        # todo: format data properly
        # todo: track request costs...

        # todo: return a tuple where the first element is the header
        #  and the second in the list of all the values

        if interval == "1d":
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

            self._num_calls[index] += 1

        else:
            epoch_to = int(Timestamp(range_end.strftime("%Y-%m-%d %H:%M:%S")).timestamp())
            epoch_from = int(Timestamp(range_start.strftime("%Y-%m-%d %H:%M:%S")).timestamp())

            resp = _rest_get(
                api_token,
                "intraday",
                symbol,
                f"&interval={interval}&from={str(epoch_from)}&to={str(epoch_to)}",
                fmt="csv")

            for c in resp.text.split("\n")[1::]:
                # todo: convert timestamp
                yield c[0], c[1], c[2], c[3], c[4], c[6]

            # intraday requests costs 5
            self._num_calls[index] += 5

    def get_dividends(
            self,
            index,
            symbol,
            from_: pd.Timestamp,
            to: pd.Timestamp) -> requests.Response:

        date_to = to.strftime("%Y-%m-%d")
        date_from = from_.strftime("%Y-%m-%d")

        resp = requests_get(f"{_api_url}/div/{symbol}?api_token={self._token}&fmt=json&from={date_from}&to{date_to}")

        self._num_calls[index] += 1

        return resp

    def get_splits(
            self,
            index,
            symbol,
            from_: pd.Timestamp,
            to: pd.Timestamp) -> requests.Response:
        date_to = to.strftime("%Y-%m-%d")
        date_from = from_.strftime("%Y-%m-%d")

        resp = requests_get(f"{_api_url}/splits/{symbol}?api_token={self._token}&fmt=json&from={date_from}&to{date_to}")

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


def _rest_get(
        session: requests.Session,
        api_key,
        endpoint: str = "",
        uri: str = "",
        querystring: str = "",
        fmt: str = "json") -> requests.Response:
    """Generic REST GET"""

    if endpoint.strip() == "":
        raise ValueError("endpoint is empty!")

    resp = session.get(f"{_api_url}/{endpoint}/{uri}?api_token={api_key}&fmt={fmt}{querystring}")
    # if resp.status_code != 200:
    # try:
    #     if "message" in resp.json():
    #         resp_message = resp.json()["message"]
    #     elif "errors" in resp.json():
    #         sys.exit(1)
    #     else:
    #         resp_message = ""

    # message = f"({resp.status_code}) {_api_url} - {resp_message}"

    resp.raise_for_status()

    return resp


def get_exchanges(api_key) -> requests.Response:
    """Get supported exchanges"""

    return _rest_get(api_key, "exchanges-list")


def get_exchange_symbols(api_key, exchange: str = "") -> requests.Response:
    """Get supported exchange symbols"""

    if exchange.strip() == "":
        raise ValueError("endpoint uri is empty!")

    return _rest_get(api_key, "exchange-symbol-list", exchange)