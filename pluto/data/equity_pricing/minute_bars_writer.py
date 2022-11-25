import typing

import numpy
import pandas
import trading_calendars

from pluto.data.equity_pricing import bar_writer

OHLC_RATIO = 1000

_INSERT = """INSERT INTO minute_bars (sid, open, high, low, close, volume, timestamp) VALUES """


class ClickHouseMinuteBarWriter(object):
    def __init__(self,
                 db_config: dict,
                 bundle_name: str,
                 trading_calendar: trading_calendars.TradingCalendar,
                 start_dt: pandas.Timestamp,
                 end_dt: pandas.Timestamp):
        """

        Parameters
        ----------
        db_config
        bundle_name
        trading_calendar: trading_calendars.TradingCalendar
        """
        self._db_writer = bar_writer.ClickHouseDataWriter(
            db_config,
            bundle_name,
            bar_writer.MinuteBars())

        self._trading_calendar = trading_calendar

        self._start_dt = start_dt
        self._end_dt = end_dt

        self._minutes_in_range = trading_calendar.minutes_in_range(
            self._start_dt,
            self._end_dt)

        self._written = False

    def write_cols(self,
                   sid: int,
                   dts: pandas.DatetimeIndex,
                   cols: typing.Dict[str, numpy.array]):

        if self._written:
            return

        minutes = self._minutes_in_range

        is_in = minutes.isin(dts)

        # minutes to skip
        sl = dts.slice_locs(minutes[0], minutes[-1])
        dts = dts[sl[0]:sl[1]]

        # fill any missing minutes
        extended = dts.union(minutes)

        opens = cols['open']
        highs = cols['high']
        lows = cols['low']
        closes = cols['close']
        volumes = cols['volume']

        def data_generator():
            i = 0

            for dt, has in zip(extended, is_in):
                # skipped minutes have no data...
                if not has:
                    yield {
                        'open': 0,
                        'high': 0,
                        'low': 0,
                        'close': 0,
                        'volume': 0,
                        'sid': sid,
                        'dt': dt
                    }
                else:
                    yield {
                        'open': int(opens[i] * OHLC_RATIO),
                        'high': int(highs[i] * OHLC_RATIO),
                        'low': int(lows[i] * OHLC_RATIO),
                        'close': int(closes[i] * OHLC_RATIO),
                        'volume': int(volumes[i]),
                        'sid': sid,
                        'dt': dt
                    }

                    i += 1

        self._db_writer.write(data_generator())
        self._written = True

    def advance(self, end_dt: pandas.Timestamp):
        cal = self._trading_calendar
        start_dt = cal.next_minute(self._end_dt)

        if start_dt > end_dt:
            raise ValueError()

        self.set_range(start_dt, end_dt)

    def move_back(self, dt):
        cal = self._trading_calendar

        end_dt = cal.previous_minute(self._start_dt)

        if end_dt < dt:
            raise ValueError()

        self.set_range(dt, end_dt)

    def set_range(self, start_dt: pandas.Timestamp, end_dt: pandas.Timestamp):
        self._start_dt = start_dt
        self._end_dt = end_dt

        self._minutes_in_range = self._trading_calendar.minutes_in_range(
            self._start_dt,
            self._end_dt)
        self._written = False

    def write_sid(self, sid: int, df: pandas.DataFrame):
        cols = {
            'open': df.open.values,
            'high': df.high.values,
            'low': df.low.values,
            'close': df.close.values,
            'volume': df.volume.values
        }

        dts = df.index

        self.write_cols(sid, dts, cols)
