from abc import ABC

import clickhouse_driver

import numpy as np
import trading_calendars

from zipline.data import session_bars

_REQUEST_DAILY_COLUMN = """
SELECT %(col)s, %(sid)s FROM data.daily_bars 
WHERE sid = %(sid)s day BETWEEN %(start)s AND %(end)s ORDER BY (sid, day)

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
            clickhouse_client: clickhouse_driver.Client,
            trading_calendar: trading_calendars.TradingCalendar):
        self._client = clickhouse_client
        self._trading_calendar = trading_calendar

    def set_trading_calendar(self, trading_calendar):
        self._trading_calendar = trading_calendar

    @property
    def trading_calendar(self):
        return self._trading_calendar

    @property
    def data_frequency(self):
        return 'daily'

    @property
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

        clt = self._client

        # ensure assets are sorted
        assets = list(assets)
        assets.sort()

        results = []

        msr = self._trading_calendar.minutes_for_sessions_in_range(
            start_date,
            end_date
        )

        for col in columns:
            outbuf = np.zeros(
                shape=(len(msr), len(assets)),
                dtype=np.uint32)

            m = 0
            a = 0

            for result in clt.execute_iter(
                    _REQUEST_DAILY_COLUMN,
                    {'col': col,
                     'start': start_date,
                     'end': end_date}):

                col, sid = result

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

        return results

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

