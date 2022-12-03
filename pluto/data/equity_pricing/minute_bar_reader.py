import json
import typing

import numpy as np
import pandas
import pandas as pd
import trading_calendars

import clickhouse_driver as cld

import directory.directory

from pluto.trading_calendars import compound_calendar

from zipline.data import session_bars
from zipline.data.bar_reader import (
    NoDataAfterDate,
    NoDataBeforeDate,
    NoDataOnDate,
)

_REQUEST_MINUTE_VALUE = '''
SELECT sid, dt, {} FROM minute_bars 
WHERE sid = %(sid)s AND dt = %(dt)s
'''

_REQUEST_MINUTE_COLUMN = '''
SELECT sid, dt, {} FROM minute_bars
WHERE dt = %(dt)s ORDER BY (sid, dt)
'''

_REQUEST_MINUTE_RANGE = '''
SELECT sid, dt, {} FROM minute_bars
WHERE dt BETWEEN %(start)s AND %(end)s ORDER BY (sid, dt)
'''

_REQUEST_LAST_TRADED_DT = '''
SELECT dt FROM minute_bars
WHERE sid = %(sid)s AND dt <= %(dt)s AND volume != 0 ORDER BY (sid, dt)
'''


class ClickHouseMinuteMetadata(object):
    def __init__(self,
                 root_dir: directory.directory.Directory,
                 trading_calendar: typing.Union[
                     trading_calendars.TradingCalendar,
                     compound_calendar.CompoundCalendar]):

        file = root_dir.get_directory('data').get_directory('pluto').get_file('metadata', 'json')

        # load metadata of the calendar
        with open(file.path, 'a') as f:
            # aggregate metadata
            if isinstance(trading_calendar, compound_calendar.CompoundCalendar):
                meta = json.load(f)
                start = min([meta[name]['first_session'] for name in trading_calendar.base_calendars])
                end = max([meta[name]['last_session'] for name in trading_calendar.base_calendars])
                sessions = trading_calendar.sessions_in_range(start, end)
                self.start_session = sessions[0]
                self.end_session = sessions[-1]
            else:
                metadata = json.load(f)[trading_calendar.name]
                self.start_session = metadata['first_session']
                self.end_session = metadata['last_session']


class ClickHouseMinuteBarReader(session_bars.CurrencyAwareSessionBarReader):
    def __init__(self,
                 db_config: dict,
                 bundle_name: str,
                 root_dir: directory.directory.Directory,
                 trading_calendar: typing.Union[
                     trading_calendars.TradingCalendar,
                     compound_calendar.CompoundCalendar
                 ]):

        self._client = cld.Client(
            db_config['host'],
            db_config['port'],
            password=db_config['password'],
            database=bundle_name)

        self.calendar = trading_calendar
        self._metadata = ClickHouseMinuteMetadata(
            root_dir,
            trading_calendar)

    @property
    def trading_calendar(self):
        return self.calendar

    @trading_calendar.setter
    def trading_calendar(self, calendar):
        self.calendar = calendar

    @property
    def data_frequency(self):
        return 'minute'

    def sessions(self):
        return self.calendar.all_sessions

    def currency_codes(self, sids):
        raise NotImplementedError("Currency code not supported yet")

    def load_raw_arrays(self, columns, start_dt, end_dt, sids):
        """
        Parameters
        ----------
        columns : list of str
           'open', 'high', 'low', 'close', or 'volume'
        start_dt: Timestamp
           Beginning of the window range.
        end_dt: Timestamp
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
        results = []

        client = self._client

        mir = self.calendar.minutes_in_range(start_dt, end_dt).tz_localize(None)
        shape = len(mir), len(sids)

        for col in columns:
            result_generator = iter(client.execute_iter(
                _REQUEST_MINUTE_RANGE.format(col),
                {'start': mir[0], 'end': mir[-1]},
                chunk_size=10000)
            )

            asset, dt, value = next(result_generator)

            not_vol = col != 'volume'

            if not_vol:
                out = np.full(shape, np.nan)
            else:
                out = np.zeros(shape, dtype=np.uint32)

            j = 0

            for sid in sorted(sids):
                while True:
                    if sid == asset:
                        if not_vol:
                            # if value:
                            out[mir.get_loc(dt), j] = value * 0.001
                        else:
                            out[mir.get_loc(dt), j] = value
                    try:
                        asset, dt, value = next(result_generator)

                        if asset > sid:
                            break

                    except StopIteration:
                        break
                j += 1
            results.append(out)

        return results

    @property
    def last_available_dt(self):
        """
        Returns
        -------
        dt : pd.Timestamp
            The last session for which the reader can provide data.
        """
        return self._metadata.end_session

    @property
    def first_trading_day(self):
        """
        Returns
        -------
        dt : pd.Timestamp
            The first trading day (session) for which the reader can provide
            data.
        """
        return self._metadata.start_session

    def get_value(self, sid: int, dt: pandas.Timestamp, field: str):
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
        # remove timezone
        dt = dt.tz_localize(None)

        data = self._client.execute(
            _REQUEST_MINUTE_VALUE.format(field),
            {'sid': sid, 'dt': dt})

        if not data:
            raise NoDataOnDate("no data for {} at datetime: {}".format(sid, dt))

        if field != 'volume':
            value = data[0][2]
            if value:
                return value * 0.001
            return np.nan

        return data[0][2]

    def get_last_traded_dt(self, asset, dt: pandas.Timestamp):
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
        # todo: last traded dt is the last close dt. So, if we have

        data = self._client.execute(
            _REQUEST_LAST_TRADED_DT,
            {'sid': asset.sid, 'dt': dt.tz_localize(None)}
        )

        if not data:
            return pd.NaT
        else:
            return pd.Timestamp(data[0][0])

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._client.disconnect()
