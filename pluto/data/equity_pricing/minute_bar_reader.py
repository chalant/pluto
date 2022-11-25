import numpy as np
import trading_calendars

from intervaltree import IntervalTree

import clickhouse_driver as cld

from zipline.gens.sim_engine import NANOS_IN_MINUTE

from zipline.data._minute_bar_internal import (
    minute_value,
    find_position_of_minute,
    find_last_traded_position_internal
)

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
SELECT sid, datetime, {} FROM minute_bars
WHERE dt = %(dt)s ORDER BY (sid, datetime)
'''

_REQUEST_MINUTE_RANGE = '''
SELECT sid, datetime, {} FROM minute_bars
WHERE dt BETWEEN %(start)s AND %(end)s ORDER BY (sid, datetime)
'''


class ClickHouseMinuteBarReader(session_bars.CurrencyAwareSessionBarReader):
    def __init__(self,
                 db_config: dict,
                 bundle_name: str,
                 trading_calendar: trading_calendars.TradingCalendar):
        """

        Parameters
        ----------
        db_config
        bundle_name
        trading_calendar: trading_calendars.TradingCalendar
        """

        self._client = cld.Client(
            db_config['host'],
            db_config['port'],
            password=db_config['password'],
            database=bundle_name
        )

        self.calendar = trading_calendar

        # self._schedule = self.calendar.schedule
        # self._market_opens = self._schedule.market_open
        # self._market_open_values = self._market_opens.values.astype('datetime64[m]').astype(np.int64)
        # self._market_closes = self._schedule.market_close
        # self._market_close_values = self._market_closes.values.astype('datetime64[m]').astype(np.int64)

        # self._minutes_per_day = 390
        # self._interval_tree = self._minute_exclusion_tree()

    @property
    def trading_calendar(self):
        return self.calendar

    @trading_calendar.setter
    def trading_calendar(self, calendar):
        # recompute everything

        self.calendar = calendar

        # self._schedule = self.calendar.schedule
        # self._market_opens = self._schedule.market_open
        # self._market_open_values = self._market_opens.values. \
        #     astype('datetime64[m]').astype(np.int64)
        # self._market_closes = self._schedule.market_close
        # self._market_close_values = self._market_closes.values. \
        #     astype('datetime64[m]').astype(np.int64)
        # self._interval_tree = self._minute_exclusion_tree()

    @property
    def data_frequency(self):
        return 'minute'

    def sessions(self):
        pass

    def currency_codes(self, sids):
        pass

    # def _minutes_to_exclude(self):
    #     """
    #     Calculate the minutes which should be excluded when a window
    #     occurs on days which had an early close, i.e. days where the close
    #     based on the regular period of minutes per day and the market close
    #     do not match.
    #
    #     Returns
    #     -------
    #     List of DatetimeIndex representing the minutes to exclude because
    #     of early closes.
    #     """
    #     market_opens = self._market_opens.values.astype('datetime64[m]')
    #     market_closes = self._market_closes.values.astype('datetime64[m]')
    #     minutes_per_day = (market_closes - market_opens).astype(np.int64)
    #     early_indices = np.where(
    #         minutes_per_day != self._minutes_per_day - 1)[0]
    #     early_opens = self._market_opens[early_indices]
    #     early_closes = self._market_closes[early_indices]
    #     minutes = [(market_open, early_close)
    #                for market_open, early_close
    #                in zip(early_opens, early_closes)]
    #     return minutes
    #
    # def _minute_exclusion_tree(self):
    #     """
    #     Build an interval tree keyed by the start and end of each range
    #     of positions should be dropped from windows. (These are the minutes
    #     between an early close and the minute which would be the close based
    #     on the regular period if there were no early close.)
    #     The value of each node is the same start and end position stored as
    #     a tuple.
    #
    #     The data is stored as such in support of a fast answer to the question,
    #     does a given start and end position overlap any of the exclusion spans?
    #
    #     Returns
    #     -------
    #     IntervalTree containing nodes which represent the minutes to exclude
    #     because of early closes.
    #     """
    #     itree = IntervalTree()
    #     for market_open, early_close in self._minutes_to_exclude():
    #         start_pos = self._find_position_of_minute(early_close) + 1
    #         end_pos = (
    #             self._find_position_of_minute(market_open)
    #             +
    #             self._minutes_per_day
    #             -
    #             1
    #         )
    #         data = (start_pos, end_pos)
    #         itree[start_pos:end_pos + 1] = data
    #     return itree

    # def _find_position_of_minute(self, minute_dt):
    #     """
    #     Internal method that returns the position of the given minute in the
    #     list of every trading minute since market open of the first trading
    #     day. Adjusts non market minutes to the last close.
    #
    #     ex. this method would return 1 for 2002-01-02 9:32 AM Eastern, if
    #     2002-01-02 is the first trading day of the dataset.
    #
    #     Parameters
    #     ----------
    #     minute_dt: pd.Timestamp
    #         The minute whose position should be calculated.
    #
    #     Returns
    #     -------
    #     int: The position of the given minute in the list of all trading
    #     minutes since market open on the first trading day.
    #     """
    #     return find_position_of_minute(
    #         self._market_open_values,
    #         self._market_close_values,
    #         minute_dt.value / NANOS_IN_MINUTE,
    #         self._minutes_per_day,
    #         False,
    #     )

    # def _exclusion_indices_for_range(self, start_idx, end_idx):
    #     """
    #     Returns
    #     -------
    #     List of tuples of (start, stop) which represent the ranges of minutes
    #     which should be excluded when a market minute window is requested.
    #     """
    #     itree = self._interval_tree
    #     if itree.overlaps(start_idx, end_idx):
    #         ranges = []
    #         intervals = itree[start_idx:end_idx]
    #         for interval in intervals:
    #             ranges.append(interval.data)
    #         return sorted(ranges)
    #     else:
    #         return None

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

        # start_idx = self._find_position_of_minute(start_dt)
        # end_idx = self._find_position_of_minute(end_dt)
        #
        # num_minutes = (end_idx - start_idx + 1)
        #
        # indices_to_exclude = self._exclusion_indices_for_range(start_idx, end_idx)
        #
        # if indices_to_exclude is not None:
        #     for excl_start, excl_stop in indices_to_exclude:
        #         length = excl_stop - excl_start + 1
        #         num_minutes -= length

        client = self._client

        mir = self.calendar.minutes_in_range(start_dt, end_dt)
        shape = len(mir), len(sids)

        for col in columns:
            result_generator = iter(client.execute_iter(
                _REQUEST_MINUTE_RANGE.format(col),
                {'start': start_dt, 'end': end_dt}))

            asset, dt, value = next(result_generator)

            not_vol = col != 'volume'

            if not_vol:
                out = np.full(shape, np.nan)
            else:
                out = np.zeros(shape, dtype=np.uint32)

            j = 0

            for sid in sids:
                while True:
                    if sid == asset:
                        if not_vol:
                            if value:
                                out[mir.get_loc(dt), j] = value * 0.001
                            else:
                                out[mir.get_loc(dt), j] = np.nan
                        else:
                            out[mir.get_loc(dt), j] = value
                    try:
                        asset, dt, value = next(result_generator)

                        if sid > asset:
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
        pass

    @property
    def trading_calendar(self):
        """
        Returns the zipline.utils.calendar.trading_calendar used to read
        the data.  Can be None (if the writer didn't specify it).
        """
        return self.calendar

    @property
    def first_trading_day(self):
        """
        Returns
        -------
        dt : pd.Timestamp
            The first trading day (session) for which the reader can provide
            data.
        """
        return

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
        # minute_val = dt.value
        # closes = self._market_close_values
        # opens = self._market_open_values
        #
        # idx = np.searchsorted(closes, minute_val, side='right')
        # close = closes[idx]
        #
        # idx = np.searchsorted(opens, minute_val, side='left')
        # open_ = opens[idx]
        #
        # if minute_val > close or minute_val < open_:
        #     raise NoDataOnDate("no data for {} at datetime: {}".format(sid, dt))

        data = self._client.execute(
            _REQUEST_MINUTE_VALUE.format(field),
            {'sid': sid, 'dt': dt})

        if not data:
            raise NoDataOnDate("no data for {} at datetime: {}".format(sid, dt))

        if field != 'volume':
            value = data[2]
            if value:
                return value * 0.001
            return np.nan

        return data[2]

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