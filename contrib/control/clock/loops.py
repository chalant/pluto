import abc
import time


import pandas as pd

from datetime import datetime, timedelta

from contrib.control.clock import clock_engine
from contrib.trading_calendars import calendar_utils as cu


def _get_minutes(clocks):
    # combine the minutes of all the clocks minutes
    minutes = pd.DatetimeIndex()
    for clock in clocks:
        minutes = minutes.union(clock.all_minutes)

    return minutes

class Loop(abc.ABC):
    def run(self):
        pass

    def _get_minutes(self, clocks):
        # combine the minutes of all the clocks minutes
        minutes = pd.DatetimeIndex()
        for clock in clocks:
            minutes = minutes.union(clock.all_minutes)

        return minutes

    def get_clock(self, exchange):
        cl = self._clocks.get(exchange, None)
        if not cl:
            self._clocks[exchange] = cl = self._get_clock(exchange)
        return cl

    @abc.abstractmethod
    def _get_clock(self, exchange):
        raise NotImplementedError

class MinuteSimulationLoop(Loop):
    def __init__(self, start_dt, end_dt):
        self._start_dt = start_dt
        self._end_dt = end_dt
        self._clocks = []

    def run(self):
        clocks = self._clocks

        for dt in _get_minutes(clocks):
            for clock in clocks:
                clock.update(dt, dt)

    def _get_clock(self, exchange):
        self._clock

class MinuteLiveLoop(Loop):
    def __init__(self):
        self._clocks = {}
        #todo: we need a proto_calendar database or directory => hub?

    def _get_seconds(self, time_delta):
        return time_delta.total_seconds()

    def run(self):
        ntp_stats = self._ntp_client.request(self._ntp_server_address)
        offset = ntp_stats.offset

        clocks = self._clocks.values()
        #get the earliest start timestamp
        start_ts = min([clock.all_sessions[0] for clock in clocks])

        delta_seconds = self._get_seconds(start_ts - pd.Timestamp.utcfromtimestamp(time.time() + offset))

        minutes = _get_minutes(clocks)

        if delta_seconds > 0:
            time.sleep(self._get_seconds(start_ts - pd.Timestamp.utcfromtimestamp(time.time() + offset)))

        elif delta_seconds < 0:
            # sleep until the next open day.
            t0 = time.time()
            start_ts = min([clock.all_sessions[1] for clock in clocks])
            self._load_attributes(start_ts)
            time.sleep(
                self._get_seconds(
                    pd.Timestamp(start_ts) -
                    pd.Timestamp.utcfromtimestamp(time.time() + offset)) - time.time() - t0)

        while True:
            #todo: periodically update the offset of the clock...
            #todo: periodically check for calendar updates
            t0 = time.time()
            try:
                #get the next expected minute
                dt = next(minutes)
                #for each clock send the current time
                for cl in clocks.values():
                    cl.update(pd.Timestamp.utcfromtimestamp(time.time() + offset), dt)
            except StopIteration:
                #todo: reload the clocks with new start date etc.
                pass
            #sleep for one minute (adjust for execution time)
            time.sleep(60 - time.time() + t0)

    def get_clock(self, exchange):
        clocks = self._clocks
        cl = clocks.get(exchange, None)
        if not cl:
            #todo: create clock
            return
        else:
            return cl

    def _load_attributes(self, start_dt, proto_calendar, minute_emission=False):
        self._start_dt = start_dt

        self._calendar = cal = cu.from_proto_calendar(proto_calendar, start_dt)

        self._end_dt = end_dt = cal.all_sessions[-1]

        self._time_idx = -1

        self._current_generator = self._egf.get_event_generator(
            cal, start_dt, end_dt,
            datetime.combine(datetime.min, cal.open_time) - timedelta(minutes=15),
            minute_emission
        )




