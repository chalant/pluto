import collections
import threading

from pluto.coms.utils import conversions
from pluto.control.clock import clock
from pluto.trading_calendars import calendar_utils as cu
from pluto.control.clock.utils import get_generator

from dev import events

from protos import clock_pb2


class SimulationLoop(object):
    def __init__(self, start_dt, end_dt, frequency='daily'):

        self._start_dt = start_dt
        self._end_dt = end_dt

        self._frequency = frequency

        self._clocks = clocks = {}

        self._calendar = cu.get_calendar_in_range(
            '24/7',
            start_dt,
            end_dt)

        self._num_clocks = len(clocks)

        self._init_flag = False
        self._start_flag = False
        self._bfs_flag = False

        self._control_modes = {}

        self._execution_lock = threading.Lock()
        self._to_execute = collections.deque()

        self._processes = []

    @property
    def start_dt(self):
        return self._start_dt

    def start(self, process):

        # self._add_clock(calendar_name)

        calendar = self._calendar
        clocks = self._clocks.values()

        for ts, global_event in get_generator(
                calendar,
                calendar.sessions,
                frequency=self._frequency):
            signals = []

            for cl in clocks:
                res = cl.update(ts)

                if res:
                    cts, evt = res

                    signals.append((cts, evt, cl.calendar))

            process.clock_update(ts, global_event, signals)

            yield 0

    def add_clock(self, calendar_name):
        self._clocks[calendar_name] = self._create_clock(
            calendar_name)

    def stop(self):
        pass

    def _create_clock(self, calendar_name):
        return clock.Clock(
            calendar_name,
            self._start_dt,
            self._end_dt,
            minute_emission=True)

    def _get_clocks(self, calendar_names):
        clocks = self._clocks
        for name in calendar_names:
            cl = clocks.get(name, None)

            if not cl:
                # create clock, put it in the queue and return it.
                # the clock will be activated on the next loop iteration
                cl = self._create_clock(name)
                clocks[name] = cl
