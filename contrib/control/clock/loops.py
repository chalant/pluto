import abc
import time

import ntplib
import pandas as pd
import threading
import itertools
import numpy as np

import queue

from datetime import datetime, timedelta
import datetime

from contrib.control.clock.clock import StopExecution

from contrib.trading_calendars import calendar_utils as cu
from contrib.control.clock import clock


def _get_minutes(clocks, session_length):
    def _dt_filter(gen):
        for dt, evt in gen:
            yield dt

    # returns minutes within a slice of all the sessions
    return pd.DatetimeIndex(set(_dt_filter(itertools.chain(
        *[clock.get_generator(clock.get_sessions(session_length))
          for clock in clocks])))).sort_values()


# todo: the run method of the loop must be threadsafe

class Loop(abc.ABC):
    def run(self):
        pass

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
        # todo: start_dt and end_dt must be midnight utc
        self._start_dt = start_dt
        self._end_dt = end_dt
        self._clocks = []

    def run(self):
        clocks = self._clocks

        # gets minutes within a range
        minutes = _get_minutes(clocks, 10)

        minute_idx = 0

        cur_exp_dt = minutes[minute_idx]

        for dt in _get_minutes(clocks):
            for clock in clocks:
                clock.update(dt, dt)

    def get_clock(self, exchange):
        self._clocks.append(self._create_clock(exchange))

    def _create_clock(self, exchange):
        return clock.Clock(
            exchange,
            # todo: if the current date time is above the open time, we need to move to the next session.
            self._start_dt,
            self._end_dt,
            minute_emission=True)


class TestMinuteLiveLoop(object):
    # todo: Create tests for the simulation loop...
    def __init__(self):
        # todo: we need a proto_calendar database or directory => hub?
        self._pending_clocks = queue.Queue()
        self._clocks = {}
        self._start = st = pd.Timestamp(pd.Timestamp.combine(pd.Timestamp.utcnow(), datetime.time.min), tz='UTC')
        self._end_dt = st + pd.Timedelta(days=30)

    def get_clocks(self, exchanges):
        clocks = self._clocks
        pending = []
        results = []
        for exchange in exchanges:
            cl = clocks.get(exchange, None)
            if not cl:
                # create clock, put it in the queue and return it.
                # the clock will be activated on the next loop iteration
                cl = self._create_clock(exchange)
                pending.append(cl)
            results.append(cl)
        self._pending_clocks.put(pending)
        return results

    def _create_clock(self, exchange):
        return clock.Clock(
            exchange,
            self._start,
            self._end_dt,
            minute_emission=True)

    def run(self, clocks):
        # todo: we need to test this. How? simulate time?
        # the loop is responsible of synchronizing the real time real time with the expected time

        # todo: check for zero index induced errors, especially for slices.

        # gets minutes within a range
        minutes = _get_minutes(clocks, 10)

        minute_idx = 0

        cur_exp_dt = minutes[minute_idx]
        yield cur_exp_dt

        t0 = time.time()

        start = pd.Timestamp.utcnow()

        # synchronize with the next expected minute

        def now(t0):
            return start + pd.Timedelta(seconds=time.time() - t0)

        while True:
            t1 = time.time()
            if self._get_seconds(cur_exp_dt - now(t0)) > 0:
                # sleep until the next expected minute
                break

            elif self._get_seconds(cur_exp_dt - now(t0)) < 0:
                minute_idx += 1
                # process pending clocks
                # add new clocks each new session.

                # todo: what if sessions changed here? We lose track of the minute index...
                if self._update_clocks():
                    minute_idx = 0
                    # reload minutes
                    minutes = _get_minutes(clocks, 10)
                    # search for the correct minute index since we probably haven't changed sessions
                    while minutes[minute_idx] < cur_exp_dt:
                        minute_idx += 1
                    cur_exp_dt = minutes[minute_idx]
                else:
                    try:
                        cur_exp_dt = minutes[minute_idx]
                    except IndexError:
                        # we've reached the last minute of the current last session.
                        # reset minute_idx
                        minutes = _get_minutes(clocks, 10)
                        minute_idx = 0
                        cur_exp_dt = minutes[minute_idx]
                t0 = t1
            else:
                break

        print('Starting at: {}'.format(cur_exp_dt))

        i = 0
        prev_t = 0
        # start the loop
        while True:
            # print(cur_exp_dt)
            t1 = time.time()
            # todo: periodically check for calendar updates
            # for each clock send the current time and the "expected" time
            try:
                for cl in clocks:
                    cl.update(now(t0), cur_exp_dt)
            except StopExecution:
                break

            # todo: update calendar if there is a calendar update.

            minute_idx += 1

            # process pending clocks each iteration
            if self._update_clocks():
                minute_idx = 0
                # reload minutes
                minutes = _get_minutes(clocks, 10)
                # search for the correct minute index since we probably have changed sessions
                while minutes[minute_idx] < cur_exp_dt:
                    minute_idx += 1
                cur_exp_dt = minutes[minute_idx]
            else:
                try:
                    cur_exp_dt = minutes[minute_idx]
                except IndexError:
                    print('loading new minutes at: {}'.format(cur_exp_dt))
                    minutes = _get_minutes(clocks, 10)
                    minute_idx = 0
                    cur_exp_dt = minutes[minute_idx]
                    print('current datetime: {}'.format(cur_exp_dt))
            yield cur_exp_dt

            # todo: what if we "overshoot" the expected datetime ?
            # wait until the next expected datetime

            start += cur_exp_dt - now(t0)
            t0 = t1
            i += 1

            # prev_t = prev_t + (time.time() - t0)
            # print('Loop time average: {}'.format(prev_t / i))
            # print('Current datetime: {}'.format(cur_exp_dt))

    def _update_clocks(self):
        try:
            for clock in self._pending_clocks.get_nowait():
                self._clocks[clock.exchange] = clock
            return True
        except queue.Empty:
            return False

    def _get_seconds(self, time_delta):
        return time_delta.total_seconds()


class MinuteLiveLoop(object):
    # todo: should this be a singleton?
    def __init__(self, ntp_server_address=None):
        self._clocks = {}
        # todo: we need a proto_calendar database or directory => hub?
        self._pending_clocks = []
        self._start_dt = pd.Timestamp.combine(pd.Timestamp.utcnow(), datetime.time.min)

        self._ntp_client = ntplib.NTPClient()
        self._offset = 0
        self._ntp_server_address = 'pool.ntp.org' if not ntp_server_address else ntp_server_address

        self._thread = threading.Thread(self._run)
        self._lock = threading.Lock()

        self._event = threading.Event()
        self._stop = False

    def _get_seconds(self, time_delta):
        return time_delta.total_seconds()

    def _get_offset(self):
        ntp_stats = self._ntp_client.request(self._ntp_server_address)
        return ntp_stats.offset

    def _run(self):
        # todo: should we use an "always open calendar"?
        # todo: handle interrupt signals => interrupt sleep etc.
        #  => change time.sleep to thread.Event wait with a time out. That way, we can instanly interrupt
        #  the program.
        # the loop is responsible of synchronizing the real time real time with the expected time
        clocks = self._clocks.values()
        event = self._event

        if not clocks:
            # blocks until clocks are available
            for clock in self._pending_clocks.get():
                self._clocks[clock.exchange] = clock

        offset = self._get_offset()
        # todo: check for zero index induced errors, especially for slices.

        # gets minutes within a range
        minutes = _get_minutes(clocks, 10)

        tick_counter = 0

        minute_idx = 0

        cur_exp_dt = minutes[minute_idx]

        # synchronize with the next expected minute

        def utc_now():
            return pd.Timestamp(pd.Timestamp.utcfromtimestamp(time.time() + offset), tz='UTC')

        while not self._stop:
            if self._get_seconds(cur_exp_dt - utc_now()) > 0:
                # sleep until the next expected minute
                event.wait(self._get_seconds(cur_exp_dt - utc_now()))
                break

            elif self._get_seconds(cur_exp_dt - utc_now()) < 0:
                minute_idx += 1
                # process pending clocks
                # add new clocks each new session.

                # todo: what if sessions changed here? We lose track of the minute index...
                if self._update_clocks():
                    minute_idx = 0
                    # reload minutes
                    minutes = _get_minutes(clocks, 10)
                    # search for the correct minute index since we probably haven't changed sessions
                    while minutes[minute_idx] < cur_exp_dt:
                        minute_idx += 1
                    cur_exp_dt = minutes[minute_idx]
                else:
                    try:
                        cur_exp_dt = minutes[minute_idx]
                    except IndexError:
                        # we've reached the last minute of the current last session.
                        # reset minute_idx
                        minutes = _get_minutes(clocks, 10)
                        minute_idx = 0
                        cur_exp_dt = minutes[minute_idx]
            else:
                break

        # start the loop
        while not self._stop:
            # todo: periodically check for calendar updates
            # for each clock send the current time and the "expected" time
            for cl in clocks:
                cl.update(utc_now(), cur_exp_dt)

            tick_counter += 1

            # update offset every 5 "tick"
            if tick_counter == 5:
                offset = self._get_offset()
                tick_counter = 0

            # todo: update calendar if there is a calendar update.

            minute_idx += 1

            # process pending clocks each iteration
            if self._update_clocks():
                minute_idx = 0
                # reload minutes
                minutes = _get_minutes(clocks, 10)
                # search for the correct minute index since we probably have changed sessions
                while minutes[minute_idx] < cur_exp_dt:
                    minute_idx += 1
                cur_exp_dt = minutes[minute_idx]
            else:
                try:
                    cur_exp_dt = minutes[minute_idx]
                except IndexError:
                    minutes = _get_minutes(clocks, 10)
                    minute_idx = 0
                    cur_exp_dt = minutes[minute_idx]

            # todo: what if we "overshoot" the expected datetime ?
            # wait until the next expected datetime
            event.wait(self._get_seconds(cur_exp_dt - pd.Timestamp.utcfromtimestamp(time.time() + offset)))

    def _update_clocks(self):
        #protect against race conditions, since the loop is running in a different thread
        with self._lock:
            try:
                for clock in self._pending_clocks.pop():
                    self._clocks[clock.exchange] = clock
                return True
            except IndexError:
                return False

    def run(self):
        #todo: should we protect against race conditions?
        #ingore further calls it the thread is already running
        try:
            self._thread.start()
        except RuntimeError:
            pass

    def stop(self, liquidate=False):
        self._stop = True
        self._event.set()

    def get_clocks(self, exchanges):
        #protect against race conditions since we're creating one clock per exchange
        with self._lock:
            clocks = self._clocks
            pending = []
            results = []
            for exchange in exchanges:
                cl = clocks.get(exchange, None)
                if not cl:
                    # create clock, put it in the queue and return it.
                    # the clock will be activated on the next loop iteration
                    cl = self._create_clock(exchange)
                    pending.append(cl)
                results.append(cl)
            self._pending_clocks.append(pending)
            return results

    def _create_clock(self, exchange):
        return clock.Clock(
            exchange,
            # todo: if the current date time is above the open time, we need to move to the next session.
            pd.Timestamp(pd.Timestamp.combine(pd.Timestamp.utcnow(), datetime.time.min), tz='UTC'),
            minute_emission=True)

    def _get_seconds(self, time_delta):
        return time_delta.total_seconds()
