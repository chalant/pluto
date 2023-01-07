import typing
import json

import numpy as np
import pandas
import pandas as pd
import trading_calendars

import directory.directory

from pluto.data.equity_pricing import bar_writer
from pluto.data import utils

OHLC_RATIO = 1000

_GET_SIDS_FOR_SESSION = '''
    SELECT sid FROM daily_bars
    WHERE dt = %(dt)s ORDER BY (sid, dt)
'''

_GET_MINUTE_BARS_IN_RANGE = '''
    SELECT sid, dt, open, high, low, close FROM minute_bars
    WHERE dt BETWEEN %(open)s and %(close)s ORDER BY (sid, dt)
'''

_GET_ALL_SIDS_AND_DT_DAILY = '''
    SELECT sid, dt FROM daily_bars ORDER BY (sid, dt)
'''


def daily_bar_generator(
        sessions: pd.DatetimeIndex,
        trading_calendar: trading_calendars.TradingCalendar,
        db_config: dict,
        bundle_name: str):
    with utils.get_client(db_config, bundle_name) as cl:
        for session in sessions:
            open_dt, close_dt = trading_calendar.open_and_close_for_session(session)

            itr = iter(cl.execute_iter(
                _GET_MINUTE_BARS_IN_RANGE,
                {'open': open_dt, 'close': close_dt},
                chunk_size=10000))
            sid, dt, open_, high, low, close, vol = next(itr)

            op = open_
            hh = high
            ll = low

            tot_vol = vol

            while True:
                try:
                    nsid, ndt, nopen, nhigh, nlow, nclose, nvol = next(itr)

                    if nsid == sid:
                        if hh < nhigh:
                            hh = nhigh
                        if ll > nlow:
                            ll = nlow

                        tot_vol += nvol
                    else:
                        yield {
                            'sid': sid,
                            'open': op,
                            'high': hh,
                            'low': ll,
                            'close': nclose,
                            'volume': tot_vol,
                            'dt': close_dt
                        }

                        # reset values
                        hh = nhigh
                        ll = nlow
                        sid = nsid
                        tot_vol = nvol
                        op = nopen

                except StopIteration:
                    break


def lifetimes_generator(db_config, bundle_name):
    with utils.get_client(db_config, bundle_name) as cl:
        itr = iter(cl.execute_iter(
            _GET_ALL_SIDS_AND_DT_DAILY,
            chunk_size=10000))

        sid, dt = next(itr)

        while True:
            try:
                nsid, ndt = next(itr)

                if sid != nsid:
                    yield {
                        'sid': sid,
                        'start_date': dt,
                        'end_date': ndt,
                        'sign': 1}

                    # reset values
                    sid = nsid
                    dt = ndt

            except StopIteration:
                break


def lifetimes_update_generator(
        session: pd.Timestamp,
        trading_calendar: trading_calendars.TradingCalendar,
        db_config: dict,
        bundle_name: str):

    close_dt = trading_calendar.closes(session)

    with utils.get_client(db_config, bundle_name) as clt:
        with utils.get_client(db_config, bundle_name) as inner:

            lifetimes_iter = iter(inner.execute_iter(bar_writer.Lifetimes.get_assets, chunk_size=10000))
            lft_sid, start_session, end_session = next(lifetimes_iter)

            for res in clt.execute_iter(
                    _GET_SIDS_FOR_SESSION,
                    {'dt': close_dt},
                    chunk_size=10000):

                while True:
                    if res == lft_sid:
                        i = 0
                        sess = end_session
                        sign = -1

                        while i < 2:
                            yield {
                                'sid': lft_sid,
                                'start_session': start_session,
                                'end_session': sess,
                                'sign': sign
                            }

                            sess = close_dt
                            sign += 2

                    try:
                        lft_sid, start_session, end_session = next(lifetimes_iter)

                        if lft_sid > res:
                            break

                    except StopIteration:
                        break


class ClickHouseOverWriteError(Exception):
    pass


class ClickHouseWriteError(Exception):
    pass


class ClickHouseMinuteBarWriter(object):
    def __init__(self,
                 db_config: dict,
                 bundle_name: str,
                 metadata,
                 trading_calendar: trading_calendars.TradingCalendar):

        self._trading_calendar = trading_calendar

        self._metadata = metadata

        self._minutes_in_range = None

        self._db_config = db_config
        self._bundle_name = bundle_name

    @staticmethod
    def _convert_to_int(value: float):
        if np.isnan(value):
            return 0
        return np.int(round((OHLC_RATIO * value)))

    def write_cols(self,
                   sid: int,
                   dts: pandas.DatetimeIndex,
                   cols: typing.Dict[str, np.array]):

        # fm = minutes[0]
        # lm = minutes[-1]

        # fs = self._metadata['first_session']
        # ls = self._metadata['last_session']
        #
        # if fm > fs and lm < ls:
        #     # minutes to run are within already written data
        #     raise ClickHouseOverWriteError("Cannot overwrite already written data")
        #
        # elif fm > fs and lm > ls:
        #     # todo: write from ls to lm
        #     pass
        #
        # elif fm < fs and lm < ls:
        #     # todo: write from fm to fs
        #     pass
        #
        # elif fm < fs and lm > ls:
        #     # todo write from fm to fs and from ls to lm
        #     pass

        dts = dts.tz_localize(None)

        minutes = self._trading_calendar.minutes_in_range(dts[0], dts[-1]).tz_localize(None)

        # remove invalid minutes
        # drops elements that are in dts but not in minutes
        to_drop = dts.difference(minutes)
        index_to_drop = dts.asof_locs(to_drop, np.array([True] * len(dts)))
        dts = dts.drop(to_drop)

        # minutes to skip
        sl = dts.slice_locs(minutes[0], minutes[-1])

        # dts = dts[sl[0]:sl[1]]
        # is_in = minutes.isin(dts)
        #
        # # todo: don't store missing data between minutes.
        #
        # # fill any missing minutes
        # extended = dts.union(minutes)

        # drop invalid data
        opens = np.delete(cols['open'], index_to_drop)
        highs = np.delete(cols['high'], index_to_drop)
        lows = np.delete(cols['low'], index_to_drop)
        closes = np.delete(cols['close'], index_to_drop)
        volumes = np.delete(cols['volume'], index_to_drop)

        def data_generator():
            i = 0

            for dt in dts:
                # skipped minutes have no data...
                # if not has:
                #     yield {
                #         'open': 0,
                #         'high': 0,
                #         'low': 0,
                #         'close': 0,
                #         'volume': 0,
                #         'sid': sid,
                #         'dt': dt
                #     }
                # else:

                # if has:
                yield {
                    'open': self._convert_to_int(opens[i]),
                    'high': self._convert_to_int(highs[i]),
                    'low': self._convert_to_int(lows[i]),
                    'close': self._convert_to_int(closes[i]),
                    'volume': int(volumes[i]),
                    'sid': sid,
                    'dt': dt
                }

                i += 1

        bar_writer.write(
            data_generator(),
            self._db_config,
            self._bundle_name,
            bar_writer.MinuteBars)

        metadata = self._metadata[self._trading_calendar.name]

        if metadata['first_session'] and metadata['last_session']:
            first = pd.Timestamp(metadata['first_session'])
            last = pd.Timestamp(metadata['last_session'])

            if minutes[0] < first:
                metadata['first_session'] = minutes[0]

            if minutes[-1] > last:
                metadata['last_session'] = minutes[-1]
        else:
            metadata['first_session'] = minutes[0]
            metadata['last_session'] = minutes[-1]

        metadata['bars_written'] = True

        self.flush()

    # def advance(self, end_dt: pandas.Timestamp):
    #     cal = self._trading_calendar
    #     start_dt = cal.next_minute(self._end_dt)
    #
    #     if start_dt > end_dt:
    #         raise ValueError()
    #
    #     self.set_range(start_dt, end_dt)
    #
    # def move_back(self, dt):
    #     cal = self._trading_calendar
    #
    #     end_dt = cal.previous_minute(self._start_dt)
    #
    #     if end_dt < dt:
    #         raise ValueError()
    #
    #     self.set_range(dt, end_dt)

    def set_range(self, start_dt: pandas.Timestamp, end_dt: pandas.Timestamp):
        self._minutes_in_range = self._trading_calendar.minutes_in_range(
            start_dt,
            end_dt)

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

    def write_lifetimes(self):
        if not self._metadata[self._trading_calendar.name]['bars_written']:
            raise ClickHouseWriteError('Bars data table have not been initialized')

        # todo: we need an initial write for asset lifetimes
        # we iterate each sid from minute_bars, we get the first dt, and last dt from minutes bars

        self._metadata[self._trading_calendar.name]['lifetimes_written'] = True
        self.flush()

    def update_lifetimes(self, session: pd.Timestamp):
        if not self._metadata[self._trading_calendar.name]['lifetimes_written']:
            raise ClickHouseWriteError('Lifetimes table have not been initialized')

        # if we have any minute data for a given asset for the given session,
        # then the asset was alive for that session.

        # todo: we need to first aggregate daily data, then update lifetimes.
        #  because if we aggregate daily data, we can always either have nothing
        #  or something for that session.

        # note: we assume that we've already written bar data.
        # so we query assets with data corresponding to the session

        session = session.tz_localize(None)

        db_config = self._db_config
        bundle_name = self._bundle_name

        bar_writer.write(
            lifetimes_update_generator(
                session,
                self._trading_calendar,
                db_config,
                bundle_name),
            db_config,
            bundle_name,
            bar_writer.Lifetimes
        )

    def aggregate_daily(self, sessions):
        # aggregates minute bars into daily bars.

        trading_calendar = self._trading_calendar

        meta = self._metadata[trading_calendar.name]
        lifetimes_written = meta['lifetimes_written']

        db_config = self._db_config
        bundle_name = self._bundle_name

        # write aggregated bars
        bar_writer.write(
            daily_bar_generator(
                sessions,
                trading_calendar,
                db_config,
                bundle_name),
            db_config,
            bundle_name,
            bar_writer.DailyBars)

        if not lifetimes_written:
            # populate initial lifetimes table.
            bar_writer.write(
                lifetimes_generator(db_config, bundle_name),
                db_config,
                bundle_name,
                bar_writer.Lifetimes)

            meta['lifetimes_written'] = True
            self.flush()

    def flush(self):
        # over-write metadata
        # with open(self._metadata_file.path, 'w') as f:
        #     json.dump(self._metadata, f)
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.flush()
