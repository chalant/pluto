from datetime import time

import pandas as pd

from pluto.trading_calendars import calendar_utils as cu
from pluto.control.clock.utils import get_generator


class StopExecution(Exception):
    pass


class FakeClock(object):
    def update(self, dt):
        return ()


# a clock is responsible of filtering time events from the loop.
class Clock(object):
    def __init__(self, exchange, start_dt, end_dt=None, minute_emission=False, max_reloads=0):

        self._exchange = exchange

        self._start_dt = start_dt
        self._end_dt = end_dt

        self._calendar = None
        self._minute_emission = minute_emission

        self._load_attributes(start_dt, end_dt)

        # next evt and dt
        self._nxt_dt, self._nxt_evt = self._next_(start_dt)

        self._reload_flag = False

    @property
    def exchange(self):
        return self._exchange

    def update(self, dt):
        # todo: we should consider reloading when there is a calendar update...
        # todo: should we catch a stop iteration?
        ts, evt = self._nxt_dt, self._nxt_evt
        exchange = self._exchange
        if dt >= ts:
            try:
                self._nxt_dt, self._nxt_evt = self._next_(dt)
                return ts, evt, exchange
            except StopExecution:
                return ts, evt, exchange
        else:
            # don't call next
            # clock is in advance, so will return nothing until it is in-sync
            return ()

    def _next_(self, dt):
        try:
            return next(self._generator)
        except StopIteration:
            if dt < self._end_dt:
                self._load_attributes(
                    pd.Timestamp(
                        pd.Timestamp.combine(dt + pd.Timedelta('1 day'), time.min),
                        tz='UTC'))
                return next(self._generator)
            else:
                # we've reached the end date, reload only if the clock runs forever
                if self._reload_flag:
                    self._load_attributes(
                        pd.Timestamp(
                            pd.Timestamp.combine(dt + pd.Timedelta('1 day'), time.min),
                            tz='UTC'))
                    # self._notify(real_dt, dt, clock_pb2.CALENDAR)
                    # print('Current timestamp {} Last timestamp {}'.format(dt, self._end_dt))
                    return next(self._generator)
                else:
                    raise StopExecution

    def _load_attributes(self, start_dt, end_dt=None):
        self._start_dt = start_dt

        self._calendar = cal = cu.get_calendar_in_range(self.exchange, start_dt, end_dt)

        self._end_dt = end_dt if end_dt else cal.all_sessions[-1]

        self._generator = iter(get_generator(cal, cal.all_sessions))
