import itertools

import numpy as np

import pandas as pd

from datetime import datetime, timedelta

from protos.clock_pb2 import (
    BAR,
    BEFORE_TRADING_START,
    SESSION_START,
    SESSION_END,
    MINUTE_END
)

from trading_calendars.utils.pandas_utils import days_at_time

_nanos_in_minute = np.int64(60000000000)

#todo: this is a huge bottleneck!
def get_event_generator(calendar, start_dt, end_dt, before_trading_starts_time=None, minute_emission=False):
    # loops every x frequency

    if not before_trading_starts_time:
        opn = calendar.open_times[0][1]
        dt = datetime.combine(datetime.min, opn) - timedelta(minutes=15)
        before_trading_starts_time = dt.time()

    sessions = calendar.sessions_in_range(start_dt, end_dt)
    trading_o_and_c = calendar.schedule.loc[sessions]
    market_closes = trading_o_and_c['market_close']

    bts_minutes = days_at_time(
        sessions,
        before_trading_starts_time,
        calendar.tz
    )

    if minute_emission:
        market_opens = trading_o_and_c['market_open']
        execution_opens = calendar.execution_time_from_open(market_opens)
        execution_closes = calendar.execution_time_from_close(market_closes)
    else:
        execution_closes = calendar.execution_time_from_close(market_closes)
        execution_opens = execution_closes

    market_opens_nanos = execution_opens.values.astype(np.int64)
    market_closes_nanos = execution_closes.values.astype(np.int64)

    session_nanos = sessions.values.astype(np.int64)
    bts_nanos = bts_minutes.values.astype(np.int64)

    minutes_by_session = _calc_minutes_by_session(market_opens_nanos, market_closes_nanos, session_nanos)

    for idx, session_nano in enumerate(session_nanos):
        bts_minute = pd.Timestamp(bts_nanos[idx], tz='UTC')
        regular_minutes = minutes_by_session[session_nano]

        yield pd.Timestamp(session_nano, tz='UTC'), SESSION_START

        if bts_minute > regular_minutes[-1]:
            # before_trading_start is after the last close,
            # so don't emit it
            for minute, evt in _get_minutes_for_list(
                    regular_minutes,
                    minute_emission
            ):
                yield minute, evt
        else:
            # we have to search anew every session, because there is no
            # guarantee that any two session start on the same minute
            bts_idx = regular_minutes.searchsorted(bts_minute)

            # emit all the minutes before bts_minute
            for minute, evt in _get_minutes_for_list(
                    regular_minutes[0:bts_idx],
                    minute_emission
            ):
                yield minute, evt

            yield bts_minute, BEFORE_TRADING_START

            # emit all the minutes after bts_minute
            for minute, evt in _get_minutes_for_list(
                    regular_minutes[bts_idx:],
                    minute_emission):
                yield minute, evt
        minute_dt = regular_minutes[-1]
        yield minute_dt, SESSION_END


def _get_minutes_for_list(minutes, minute_emission):
    events_to_include = [BAR, MINUTE_END] if minute_emission else [BAR]
    for status in itertools.product(minutes, events_to_include):
        yield status


def _calc_minutes_by_session(market_opens_nanos, market_closes_nanos, sessions_nanos):
    minutes_by_session = {}
    for session_idx, session_nano in enumerate(sessions_nanos):
        minutes_nanos = np.arange(
            market_opens_nanos[session_idx],
            market_closes_nanos[session_idx] + _nanos_in_minute,
            _nanos_in_minute
        )
        minutes_by_session[session_nano] = pd.to_datetime(
            minutes_nanos, utc=True, box=True
        )
    return minutes_by_session