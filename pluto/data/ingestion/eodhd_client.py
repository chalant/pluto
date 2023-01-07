from json.decoder import JSONDecodeError
import sys
from enum import Enum
from datetime import datetime
from datetime import timedelta
from re import compile as re_compile
import pandas as pd
from requests import get as requests_get

prog = re_compile(r"^[A-z0-9.]{16,32}$")

# if api_key != "demo" and not prog.match(api_key):
#     raise ValueError("API key is invalid")


_api_url = "https://eodhistoricaldata.com/api"


def _rest_get(api_key, endpoint: str = "", uri: str = "", querystring: str = "") -> pd.DataFrame():
    """Generic REST GET"""

    if endpoint.strip() == "":
        raise ValueError("endpoint is empty!")

    resp = requests_get(f"{_api_url}/{endpoint}/{uri}?api_token={api_key}&fmt=json{querystring}")

    # if resp.status_code != 200:
    #     try:
    #         if "message" in resp.json():
    #             resp_message = resp.json()["message"]
    #         elif "errors" in resp.json():
    #             sys.exit(1)
    #         else:
    #             resp_message = ""

            # message = f"({resp.status_code}) {_api_url} - {resp_message}"

    resp.raise_for_status()

    if isinstance(resp.json(), list):
        return pd.DataFrame.from_dict(resp.json())
    else:
        return pd.DataFrame(resp.json(), index=[0])


def get_exchanges(api_key) -> pd.DataFrame:
    """Get supported exchanges"""

    return _rest_get(api_key, "exchanges-list")


def get_exchange_symbols(api_key, uri: str = "") -> pd.DataFrame:
    """Get supported exchange symbols"""

    if uri.strip() == "":
        raise ValueError("endpoint uri is empty!")

    return _rest_get(api_key, "exchange-symbol-list", uri)

def get_historical_data(
            api_token: str,
            symbol: str,
            interval: str,
            range_start: str = "",
            range_end: str = "") -> dict:
        """Initiates a REST API call"""

        # default set of results
        results = 300

        # validate symbol
        prog = re_compile(r"^[A-z0-9-$\.+]{1,48}$")
        if not prog.match(symbol):
            raise ValueError(f"Symbol is invalid: {symbol}")

        # replace "." with "-" in markets
        if symbol.count(".") == 2:
            symbol = symbol.replace(".", "-", 1)

        minutes = 1
        if interval == "5m":
            minutes = 5
        elif interval == "1h":
            minutes = 60

        # validate interval

        if interval == "1d":
            prog = re_compile(r"^\d{4}\-\d{2}-\d{2}$")

            if range_end == "" or (range_end != "" and not prog.match(range_end)):
                date_to = datetime.today().date()
            else:
                date_to = datetime.strptime(range_end, "%Y-%m-%d").date()

            if range_start == "" or (range_start != "" and not prog.match(range_start)):
                date_from = date_to - timedelta(days=(results - 1))
            else:
                date_from = datetime.strptime(range_start, "%Y-%m-%d").date()

            df_data = _rest_get(
                "eod",
                symbol,
                f"&interval={interval}&from={str(date_from)}&to={str(date_to)}",
            )

        else:
            prog = re_compile(r"^\d{4}\-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")

            if range_end == "" or (range_end != "" and not prog.match(range_end)):
                epoch_to = ""
            else:
                epoch_to = int(datetime.strptime(range_end, "%Y-%m-%d %H:%M:%S").timestamp())

            if range_start == "" or (range_start != "" and not prog.match(range_start)):
                if interval == "1m":
                    epoch_from = str(DateUtils.str2epoch(str(datetime.now() - timedelta(days=1)).split(".")[0]))
                elif interval == "5m":
                    epoch_from = str(DateUtils.str2epoch(str(datetime.now() - timedelta(days=2)).split(".")[0]))
                elif interval == "1h":
                    epoch_from = str(DateUtils.str2epoch(str(datetime.now() - timedelta(days=14)).split(".")[0]))
                else:
                    epoch_from = str(int(epoch_to) - (((60 * minutes) * results) - ((60 * minutes) + 1)))
            else:
                epoch_from = int(datetime.strptime(range_start, "%Y-%m-%d %H:%M:%S").timestamp())

            if epoch_to != "":
                df_data = _rest_get(
                    "intraday",
                    symbol,
                    f"&interval={interval}&from={str(epoch_from)}&to={str(epoch_to)}",
                )
            else:
                df_data = _rest_get(
                    "intraday",
                    symbol,
                    f"&interval={interval}&from={str(epoch_from)}",
                )

            if len(df_data) == 0:
                return df_data[
                    [
                        "symbol",
                        "interval",
                        "open",
                        "high",
                        "low",
                        "close",
                        "adjusted_close",
                        "volume",
                    ]
                ]

            if range_start == "" and range_end == "":
                df_data = df_data.tail(300)
            elif range_start != "" and range_end == "":
                df_data = df_data.head(300)

        df_data["symbol"] = symbol
        df_data["interval"] = interval

        # convert dataframe to a time series
        if interval == "1d":
            tsidx = pd.DatetimeIndex(pd.to_datetime(df_data["date"]).dt.strftime("%Y-%m-%d"))
            df_data.set_index(tsidx, inplace=True)
            df_data = df_data.drop(columns=["date"])
        else:
            tsidx = pd.DatetimeIndex(pd.to_datetime(df_data["datetime"]).dt.strftime("%Y-%m-%d %H:%M:%S"))
            df_data.set_index(tsidx, inplace=True)
            df_data = df_data.drop(columns=["datetime"])

        # rename columns
        if interval != "1d":
            df_data.columns = [
                "epoch",
                "gmtoffset",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "symbol",
                "interval",
            ]

        # return dataset
        if interval == "1d":
            df_data["adjusted_close"] = df_data["adjusted_close"].astype(object)
            df_data.fillna(0, inplace=True)

            return df_data[
                [
                    "symbol",
                    "interval",
                    "open",
                    "high",
                    "low",
                    "close",
                    "adjusted_close",
                    "volume",
                ]
            ]

        # set object type to display large floats
        df_data.fillna(0, inplace=True)
        df_data["gmtoffset"] = df_data["gmtoffset"].astype(object)
        df_data["epoch"] = df_data["epoch"].astype(object)
        df_data["open"] = df_data["open"].astype(object)
        df_data["high"] = df_data["high"].astype(object)
        df_data["low"] = df_data["low"].astype(object)
        df_data["close"] = df_data["close"].astype(object)
        df_data["volume"] = df_data["volume"].astype(object)

        return df_data[
            [
                "epoch",
                "gmtoffset",
                "symbol",
                "interval",
                "open",
                "high",
                "low",
                "close",
                "volume",
            ]
        ]