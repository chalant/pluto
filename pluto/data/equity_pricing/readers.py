import collections
from abc import ABC

import pandas as pd

import clickhouse_driver

import numpy as np
import trading_calendars

from zipline.data import session_bars
from zipline.data.bar_reader import (
    NoDataAfterDate,
    NoDataBeforeDate,
    NoDataOnDate,
)

_REQUEST_DAILY_COLUMN = """
SELECT sid, day, {} FROM daily_bars 
WHERE day BETWEEN %(start)s AND %(end)s ORDER BY (sid, day)

"""


class ClickHouseMinuteBarReader(session_bars.CurrencyAwareSessionBarReader):
    def __init__(self):
        pass

    @property
    def trading_calendar(self):
        pass
    
    @property
    def data_frequency(self):
        return 'minute'

    def sessions(self):
        pass

    def currency_codes(self, sids):
        pass

    def load_raw_arrays(self, columns, start_date, end_date, assets):
        """
        Parameters
        ----------
        columns : list of str
           'open', 'high', 'low', 'close', or 'volume'
        start_date: Timestamp
           Beginning of the window range.
        end_date: Timestamp
           End of the window range.
        assets : list of int
           The asset identifiers in the window.

        Returns
        -------
        list of np.ndarray
            A list with an entry per field of ndarrays with shape
            (minutes in range, sids) with a dtype of float64, containing the
            values for the respective field over start and end dt range.
        """
        pass

    @property
    def last_available_dt(self):
        """
        Returns
        -------
        dt : pd.Timestamp
            The last session for which the reader can provide data.
        """
        pass

    @property
    def trading_calendar(self):
        """
        Returns the zipline.utils.calendar.trading_calendar used to read
        the data.  Can be None (if the writer didn't specify it).
        """
        pass

    @property
    def first_trading_day(self):
        """
        Returns
        -------
        dt : pd.Timestamp
            The first trading day (session) for which the reader can provide
            data.
        """
        pass

    def get_value(self, sid, dt, field):
        """
        Retrieve the value at the given coordinates.

        Parameters
        ----------
        sid : int
            The asset identifier.
        dt : pd.Timestamp
            The timestamp for the desired data point.
        field : string
            The OHLVC name for the desired data point.

        Returns
        -------
        value : float|int
            The value at the given coordinates, ``float`` for OHLC, ``int``
            for 'volume'.

        Raises
        ------
        NoDataOnDate
            If the given dt is not a valid market minute (in minute mode) or
            session (in daily mode) according to this reader's tradingcalendar.
        """
        pass

    def get_last_traded_dt(self, asset, dt):
        """
        Get the latest minute on or before ``dt`` in which ``asset`` traded.

        If there are no trades on or before ``dt``, returns ``pd.NaT``.

        Parameters
        ----------
        asset : zipline.asset.Asset
            The asset for which to get the last traded minute.
        dt : pd.Timestamp
            The minute at which to start searching for the last traded minute.

        Returns
        -------
        last_traded : pd.Timestamp
            The dt of the last trade for the given asset, using the input
            dt as a vantage point.
        """
        pass


class ClickHouseDailyBarReader(session_bars.CurrencyAwareSessionBarReader):
    def __init__(
            self,
            db_config,
            trading_calendar: trading_calendars.TradingCalendar,
            metadata,
            lifetimes,
            cache,
            bundle_name):

        self._config = db_config
        self._trading_calendar = trading_calendar
        self._bundle_name = bundle_name
        self._metadata = metadata

        # todo: cache lifetimes as-well!

        self._lifetimes = lifetimes
        self._cache = cache

        print("HOST", db_config['host'])

        self._client = clickhouse_driver.Client(
            db_config['host'],
            db_config['port'],
            password=db_config['password'],
            database=bundle_name
        )

    def set_trading_calendar(self, trading_calendar):
        # todo: reload and cache
        self._trading_calendar = trading_calendar

    @property
    def trading_calendar(self):
        return self._trading_calendar

    @property
    def data_frequency(self):
        return 'daily'

    @property
    def sessions(self):
        return self._trading_calendar.all_sessions

    @property
    def first_trading_day(self):
        meta = self._metadata

        try:
            pd.Timestamp(meta['first_trading_day'])
        except KeyError:
            return None

    @property
    def last_available_dt(self):
        return self.sessions[-1]

    def load_raw_arrays(self, columns, start_date, end_date, assets):
        """
        Parameters
        ----------
        columns : list of str
           'open', 'high', 'low', 'close', or 'volume'
        start_date: Timestamp
           Beginning of the window range.
        end_date: Timestamp
           End of the window range.
        assets : list of int
           The asset identifiers in the window.

        Returns
        -------
        list of np.ndarray
            A list with an entry per field of ndarrays with shape
            (minutes in range, sids) with a dtype of float64, containing the
            values for the respective field over start and end dt range.
        """

        db_config = self._config

        # ensure assets are sorted
        assets = list(assets)
        assets.sort()

        results = []

        msr = self._trading_calendar.sessions_in_range(
            start_date,
            end_date
        )

        clt = self._client

        for col in columns:
            outbuf = np.zeros(
                shape=(len(msr), len(assets)),
                dtype=np.uint32)

            m = 0
            a = 0

            # fixme: string formatting is unsafe for queries.

            for result in clt.execute_iter(
                    _REQUEST_DAILY_COLUMN.format(col),
                    {'start': start_date, 'end': end_date}):

                sid, day, col = result

                if sid == assets[a]:
                    outbuf[m, sid] = col

                    m += 1

                    if m > len(msr):
                        m = 0
                        a += 1

                        if a > len(assets):
                            break

            results.append(
                outbuf
            )

        print("RESULTS", results)

        return results

    def get_last_traded_dt(self, asset, day):
        lifetime = self._lifetimes[asset.sid]
        start = lifetime[0]
        end = lifetime[1]

        # last trade is after requested day
        if day.value < end:
            return day
        # no data before date
        elif day.value < start:
            return pd.NaT
        # no data after date
        elif day.value >= end:
            return end

    def _get_value(self, cols, dt, sid):

        lifetime = self._lifetimes[sid]
        start = lifetime[0]
        end = lifetime[1]

        try:
            value = cols[sid][dt]
        except Exception:
            raise NoDataOnDate("day={0} is outside of calendar={1}".format(
                dt, cols.index))

        if dt.value < start:
            raise NoDataBeforeDate(
                "No data on or before day={0} for sid={1}".format(
                    dt, sid))
        if dt.value > end:
            raise NoDataAfterDate(
                "No data on or after day={0} for sid={1}".format(
                    dt, sid)
            )

        return value

    def get_value(self, sid, dt, field):
        """
        Retrieve the value at the given coordinates.

        Parameters
        ----------
        sid : int
            The asset identifier.
        dt : pd.Timestamp
            The timestamp for the desired data point.
        field : string
            The OHLVC name for the desired data point.

        Returns
        -------
        value : float|int
            The value at the given coordinates, ``float`` for OHLC, ``int``
            for 'volume'.

        Raises
        ------
        NoDataOnDate
            If the given dt is not a valid market minute (in minute mode) or
            session (in daily mode) according to this reader's tradingcalendar.
        """

        clt = self._client
        sessions = self.sessions
        cache = self._cache

        if field not in cache:
            cols = collections.defaultdict(dict)

            for result in clt.execute_iter(
                    _REQUEST_DAILY_COLUMN.format(field),
                    {'start': sessions[0],
                     'end': sessions[-1]}):

                sid, day, col = result

                cols[day][sid] = col

            cols = pd.DataFrame.from_dict(
                cols,
                orient="index")
            cache.set(field, cols)
        else:
            cols = cache.get(field)

        price = self._get_value(cols, dt, sid)

        # print("Price", price, field, sid, dt)

        if field != 'volume':
            if price == 0:
                return np.nan
            else:
                return price * 0.001
        else:
            return price

    def currency_codes(self, sids):
        raise NotImplementedError

