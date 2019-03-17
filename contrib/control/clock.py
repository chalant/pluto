from enum import IntEnum
from abc import ABC, abstractmethod
from datetime import datetime, timedelta

import itertools

import numpy as np

import pandas as pd

import ntplib

import time

from trading_calendars.utils.pandas_utils import days_at_time
from contrib.trading_calendars.calendar_utils import get_calendar_in_range

'''module for events extension...'''

_nanos_in_minute = np.int64(60000000000)


class Events(IntEnum):
    BAR = 0
    SESSION_START = 1
    SESSION_END = 2
    MINUTE_END = 3
    BEFORE_TRADING_START_BAR = 4
    LIQUIDATE = 5
    STOP = 6
    INITIALIZE = 7


BAR, SESSION_START, SESSION_END, MINUTE_END, BEFORE_TRADING_START_BAR, LIQUIDATE, STOP, INITIALIZE = list(Events)


class Clock(ABC):
    @abstractmethod
    @property
    def emission_rate(self):
        raise NotImplementedError

    @abstractmethod
    @property
    def calendar(self):
        raise NotImplementedError

    @abstractmethod
    @property
    def calendar_name(self):
        raise NotImplementedError

    @abstractmethod
    def stop(self, liquidate=False):
        raise NotImplementedError

    @abstractmethod
    def __iter__(self):
        raise NotImplementedError


class MinuteEventGeneratorFactory(object):
    def __init__(self, minute_emission=False):
        self._stop = False
        self._liquidate = False
        self._minute_emission = minute_emission
        self._first_call_flag = False

    def stop(self, liquidate=False):
        self._stop = True
        self._liquidate = liquidate

    def get_event_generator(self, calendar, end_dt, before_trading_starts_time):
        # loops every x frequency
        start_ts = calendar.all_sessions[0]
        start_dt = pd.Timestamp(start_ts.date()).tz_localize(tz='UTC')

        sessions = calendar.sessions_in_range(start_dt, end_dt)
        trading_o_and_c = calendar.schedule.ix[sessions]
        market_closes = trading_o_and_c['market_close']

        before_trading_start_minutes = days_at_time(
            sessions,
            before_trading_starts_time,
            calendar.tz
        )

        minute_emission = self._minute_emission

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
        bts_nanos = before_trading_start_minutes.values.astype(np.int64)

        minutes_by_session = self._calc_minutes_by_session(market_opens_nanos, market_closes_nanos, session_nanos)

        for idx, session_nano in session_nanos:
            bts_minute = pd.Timestamp(bts_nanos[idx], tz='UTC')
            regular_minutes = minutes_by_session[session_nano]
            if not self._stop:
                if not self._first_call_flag:
                    self._first_call_flag = True
                    yield start_ts, INITIALIZE
                yield start_ts, SESSION_START

                if bts_minute > regular_minutes[-1]:
                    # before_trading_start is after the last close,
                    # so don't emit it
                    for minute, evt in self._get_minutes_for_list(
                            regular_minutes,
                            minute_emission
                    ):
                        yield minute, evt
                else:
                    # we have to search anew every session, because there is no
                    # guarantee that any two session start on the same minute
                    bts_idx = regular_minutes.searchsorted(bts_minute)

                    # emit all the minutes before bts_minute
                    for minute, evt in self._get_minutes_for_list(
                            regular_minutes[0:bts_idx],
                            minute_emission
                    ):
                        yield minute, evt

                    yield bts_minute, BEFORE_TRADING_START_BAR

                    # emit all the minutes after bts_minute
                    for minute, evt in self._get_minutes_for_list(
                            regular_minutes[bts_idx:],
                            minute_emission):
                        yield minute, evt
                minute_dt = regular_minutes[-1]
                yield minute_dt, SESSION_END
            else:
                minute_dt = regular_minutes[0]
                if self._liquidate:
                    yield minute_dt, LIQUIDATE
                else:
                    yield minute_dt, STOP

    def _get_minutes_for_list(self, minutes, minute_emission):
        events_to_include = [BAR, MINUTE_END] if minute_emission else [BAR]
        for status in itertools.product(minutes, events_to_include):
            yield status

    def _calc_minutes_by_session(self, market_opens_nanos, market_closes_nanos, sessions_nanos):
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

class MinuteClock(Clock):
    def __init__(self, minute_emission=False):
        self._minute_emission = minute_emission

    def emission_rate(self):
        if self._minute_emission:
            return 'minute'
        return 'daily'

class MinuteSimulationClock(MinuteClock):
    def __init__(self, calendar, start_dt, end_dt,
                 minute_emission=False, minute_event_generator_factory=None):
        super(MinuteSimulationClock,self).__init__(minute_emission)
        self._calendar = calendar
        self._start_dt = start_dt
        self._end_dt = end_dt
        self._minute_emission = minute_emission
        self._egf = minute_event_generator_factory if not None else MinuteEventGeneratorFactory()

        self._event_generator = self._egf.get_event_generator(
            calendar, end_dt, datetime.combine(datetime.min, calendar.open_time) - timedelta(minutes=15))

    def calendar_name(self):
        return self._calendar.name

    def stop(self, liquidate=False):
        self._egf.stop(liquidate)

    def calendar(self):
        return self._calendar

    def __iter__(self):
        for ts, evt in self._event_generator:
            yield ts, evt


class MinuteRealtimeClock(Clock):
    def __init__(self, calendar_name, ntp_server_address,
                 start_dt=None, minute_emission=True, minute_event_generator_factory=None):
        super(MinuteRealtimeClock,self).__init__(minute_emission)
        self._cal_name = calendar_name
        self._calendar = None
        self._start_dt = pd.Timestamp(pd.Timestamp.today().date(), tz='UTC') if start_dt is None else start_dt
        # the end_dt is the latest date of the calendar
        self._end_dt = None

        self._egf = minute_event_generator_factory if not None else MinuteEventGeneratorFactory(minute_emission)
        self._current_generator = None

        self._ntp_client = ntplib.NTPClient()
        self._offset_flag = False
        self._ntp_server_address = ntp_server_address
        self._minute_counter = 0
        self._offset = 0

        self._first_call_flag = False
        self._time_idx = -1

        self._load_attributes(start_dt)

    def calendar_name(self):
        return self._cal_name

    def calendar(self):
        # returns the current calendar.
        return self._calendar

    def stop(self, liquidate=False):
        self._egf.stop(liquidate)

    def _get_seconds(self, time_delta):
        return time_delta.total_seconds()

    def __iter__(self):
        # todo: should yield an initialize, signal if haven't been done yet.
        sessions = self._calendar.all_sessions
        delta_seconds = self._get_seconds(
            pd.Timestamp(sessions[0]).tz_localize(tz='UTC') -
            pd.Timestamp.utcfromtimestamp(time.time() + self._get_offset())
        )

        if delta_seconds > 0:
            time.sleep(delta_seconds)
            return self._iter()

        elif delta_seconds < 0:
            # sleep until the next open day.
            t0 = time.time()
            start_ts = pd.Timestamp(sessions[1]).tz_localize(tz='UTC')
            self._load_attributes(self._calendar.all_sessions[1])
            time.sleep(
                self._get_seconds(
                    pd.Timestamp(start_ts) -
                    pd.Timestamp.utcfromtimestamp(time.time() + self._get_offset())) -
                time.time() - t0)
            return self._iter()
        else:
            # execute now
            return self._iter()

    def _get_offset(self):
        if not self._offset_flag:
            ntp_stats = self._ntp_client.request(self._ntp_server_address)
            self._offset_flag = True
            offset = ntp_stats.offset
            self._offset = offset
            return offset
        else:
            return self._offset

    def _iter(self):
        ntp_server_address = self._ntp_server_address
        minute_counter = -1

        for ts, evt in self._current_generator:
            # for timing external computation
            t0 = time.time()
            ts = pd.Timestamp.utcfromtimestamp(time.time() + self._offset)
            yield ts, evt
            # update offset every 10 minutes
            minute_counter += 1
            if minute_counter == 10:
                minute_counter = 0
                ntp_stats = self._ntp_client.request(ntp_server_address)
                self._offset = ntp_stats.offset
            # we take into account the computation delay, so we don't "over-sleep"
            if evt == SESSION_START:
                # sleep until before trading starts
                self._time_idx += 1
                time.sleep(self._get_seconds(
                    self._calendar.opens[self._time_idx].tz_localize(tz='UTC') -
                    timedelta(minutes=15)) - ts - time.time() + t0)
            elif evt == SESSION_END:
                # sleep until next session start
                end_dt = self._end_dt
                if ts.date() == end_dt.date():
                    # re-load all attributes
                    self._load_attributes(end_dt + pd.Timedelta('1 day'))
                    time.sleep(self._calendar.all_sessions[1] - ts - time.time() + t0)
                else:
                    time.sleep(
                        self._get_seconds(self._calendar.all_sessions[self._time_idx + 1]) - ts - time.time() + t0)
            elif evt == BAR:
                # sleep for a minute
                time.sleep(60 - time.time() + t0)

    def _load_attributes(self, start_dt):
        self._start_dt = start_dt

        self._calendar = cal = get_calendar_in_range(self._cal_name, start_dt)
        self._end_dt = end_dt = cal.all_sessions[-1]

        self._time_idx = -1

        self._current_generator = self._egf.get_event_generator(
            cal, end_dt, datetime.combine(datetime.min, cal.open_time) - timedelta(minutes=15))
