import abc
import time


import pandas as pd
import threading
import itertools
import numpy as np

import queue

from datetime import datetime, timedelta

from contrib.trading_calendars import calendar_utils as cu


def _get_minutes(clocks):
    return pd.DatetimeIndex(set(itertools.chain(*[clock.all_minutes for clock in clocks])))


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
        self._pending_clocks = queue.Queue()
        self._event = threading.Event()
        self._current_session = None
        self._sessions = None

    def _get_seconds(self, time_delta):
        return time_delta.total_seconds()

    def _get_offset(self):
        ntp_stats = self._ntp_client.request(self._ntp_server_address)
        return ntp_stats.offset


    def run(self):
        offset = self._get_offset()
        clocks = self._clocks.values()

        #todo: we will overshoot the expected datetime in some cases (end bar + minute end)
        # if we overshoot, we need to execute right away.

        if clocks:
            #get the earliest start timestamp
            start_ts = min([clock.all_sessions[0] for clock in clocks])
            end_ts = max([clock.all_sessions[-1] for clock in clocks])

            minutes = _get_minutes(clocks)
            cur_exp_dt = minutes[0]

            self._current_session = cur_exp_dt

            minute_counter = 0
            # cur_exp_dt = minutes[0]
            #
            # delta_seconds = self._get_seconds(
            #     cur_exp_dt - pd.Timestamp.utcfromtimestamp(time.time() + offset))
            #
            # if delta_seconds > 0:
            #     time.sleep(self._get_seconds(
            #         cur_exp_dt - pd.Timestamp.utcfromtimestamp(time.time() + offset)))
            #
            # elif delta_seconds < 0:
            #     seconds = self._get_seconds(
            #             cur_exp_dt - pd.Timestamp.utcfromtimestamp(time.time() + offset))
            #     if seconds > 0:
            #         time.sleep(cur_exp_dt - pd.Timestamp.utcfromtimestamp(time.time() + offset))
            #     elif seconds < 0:
            #         pass

            #todo: if we reach the last session, we need to reset all the clocks.
            #      if there is a calendar update, we reset the clocks with the new calendar
            #      calendar updates are checked every end of session.
            #      we need a way to check for holidays...

            # todo: what if the current datetime is after the expected datetime?

            #sleep until the expexted datetime.

            #todo: if the current time overshoots the expected datetime, wait until the next session.
            time.sleep(self._get_seconds(cur_exp_dt - pd.Timestamp.utcfromtimestamp(time.time() + offset)))

            while True:
                #todo: periodically update the offset of the clock...
                #todo: periodically check for calendar updates

                # for each clock send the current time and the "expected" time
                for cl in clocks.values():
                    cl.update(pd.Timestamp.utcfromtimestamp(time.time() + offset), cur_exp_dt)

                minutes = minutes[1:]
                cur_exp_dt = minutes[0]

                minute_counter += 1

                # update offset every 5 minutes
                if minute_counter == 5:
                    offset = self._get_offset()
                    minute_counter = 0

                #pendig clocks are added at the end.
                # update the current session if the date changes
                if cur_exp_dt.date() > self._current_session.date():
                    # todo: pending clocks are created here
                    self._current_session = cur_exp_dt

                    # todo: check for new clocks at each iteration
                    # add new clocks
                    # todo: the start date must be the next open session. => clocks must have a reset method
                    # todo: clocks can only be started at the next open session.
                    pending = self._pending_clocks
                    try:
                        # todo: if current time is after the second minute (before trading starts), reset the clock to
                        #  start the next trading day.
                        pending_clocks = pending.get()
                        minutes.union(pending_clocks)
                    except queue.Empty:
                        pass

                #todo: what if we "overshoot" the expected datetime ?

                #until the next expected datetime
                time.sleep(self._get_seconds(cur_exp_dt - pd.Timestamp.utcfromtimestamp(time.time() + offset)))

    def get_clocks(self, exchanges):
        clocks = self._clocks
        pending = []
        results = []
        for exchange in exchanges:
            cl = clocks.get(exchange, None)
            if not cl:
                #create clock, put it in the queue and return it.
                #the clock will be activated on the next loop iteration
                cl = self._create_clock(exchange)
                pending.append(cl)
            results.append(cl)
        self._pending_clocks.put(pending)
        return results

    def _create_clock(self, exchange):
        #todo
        return

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




