from datetime import time

import pandas as pd

from pluto.data.universes import core
from pluto.control.clock.utils import get_generator
from pluto.trading_calendars import calendar_utils as cu


from dev import events

events.add_event('clock.update')
events.add_event('clock.calendar_update')


class StopExecution(Exception):
    pass


class ExplodingClock(object):
    @property
    def global_ts(self):
        raise RuntimeError("Clock was not set!")

    @property
    def session_start(self):
        raise RuntimeError("Clock was not set!")

    @property
    def session_end(self):
        raise RuntimeError("Clock was not set!")

    @property
    def calendar(self):
        raise RuntimeError("Clock was not set!")

    @property
    def calendar_name(self):
        raise RuntimeError("Clock was not set!")

    def update(self, dt):
        raise RuntimeError("Clock was not set!")


# a clock is responsible for filtering time events from the loop.
class Clock(object):
    def __init__(self,
                 calendar_name,
                 start_dt,
                 end_dt,
                 minute_emission=False):

        self._calendar_name = calendar_name

        self._start_dt = start_dt
        self._end_dt = end_dt

        self._calendar = None
        self._minute_emission = minute_emission

        self._load_attributes(start_dt, end_dt)

        self._reload_flag = False

        self._global_dt = None

    @property
    def global_ts(self):
        return self._global_dt

    @property
    def session_start(self):
        return self._start_dt

    @property
    def session_end(self):
        return self._end_dt

    @property
    def calendar(self):
        return self._calendar

    @property
    def calendar_name(self):
        return self._calendar_name

    def update(self, dt):
        # todo: we should consider reloading when there is a calendar update...
        # todo: should we catch a stop iteration?
        self._global_dt = dt

        ts, evt = self._nxt_dt, self._nxt_evt

        if dt >= ts:
            try:
                self._nxt_dt, self._nxt_evt = self._next_(dt)
                return ts, evt
            except StopExecution:
                return ts, evt
        else:
            # don't call next
            # clock is in advance, so will return nothing until it is in-sync
            return ()

    def _next_(self, dt):
        try:
            return next(self._generator)
        except StopIteration:
            # if dt < self._end_dt:
            #     self._load_attributes(
            #         pd.Timestamp(
            #             pd.Timestamp.combine(dt + pd.Timedelta('1 day'), time.min),
            #             tz='UTC'))
            #     events.post_event('clock.calendar_update', self)
            #     return next(self._generator)
            # else:
            # we've reached the end date, reload only if the clock runs forever
            if self._reload_flag:
                if dt > self._end_dt:
                    self._load_attributes(
                        pd.Timestamp(
                            pd.Timestamp.combine(dt + pd.Timedelta('1 day'), time.min),
                            tz='UTC'))
                    events.post_event(
                        'clock.calendar_update',
                        self)
                return next(self._generator)
            else:
                raise StopExecution

    def _load_attributes(self, start_dt, end_dt=None):
        self._start_dt = start_dt

        # if not end_dt:
        #     # todo: we could parametrize this... (look back period...)
        #     end_dt = start_dt + pd.Timedelta(days=150)

        # todo: we need to start from the first available data point (universe)

        self._calendar = cal = cu.get_calendar_in_range(
            self._calendar_name,
            start_dt - pd.Timedelta(days=150),
            end_dt)

        # self._start_dt = cal.first_session

        self._generator = gen = iter(get_generator(cal, cal.sessions))

        self._nxt_dt, self._nxt_evt = next(gen)

        # skip to start date.

        while start_dt.date() > self._nxt_dt.date():
            # next evt and dt
            self._nxt_dt, self._nxt_evt = next(gen)

        self._end_dt = end_dt

        # self._generator = iter(get_generator(cal, cal.sessions))
