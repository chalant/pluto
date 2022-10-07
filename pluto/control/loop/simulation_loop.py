import collections
import threading

from pluto.coms.utils import conversions
from pluto.control.clock import clock
from pluto.trading_calendars import calendar_utils as cu
from pluto.control.clock.utils import get_generator

from dev import events

from protos import clock_pb2


class SimulationLoop(object):
    def __init__(
            self,
            start_dt,
            end_dt,
            frequency='daily'):

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

        for ts, global_event in get_generator(
                calendar,
                calendar.all_sessions,
                frequency=self._frequency):

            # acquire lock so that no further commands are executed here
            # while this block is being executed
            with self._execution_lock:
                # process any cached values
                # call for any update that needs to be done before
                # anything else

                # aggregate the signals into a single signal.

                signals = []

                for cl in self._clocks.values():
                    res = cl.update(ts)

                    if res:
                        cts, evt = res

                        signals.append((cts, evt, cl.calendar))

                        # events.post_event(
                        #     'clock.controller_update',
                        #     {'ts': cts, 'evt': evt, 'calendar': cl.calendar_name}
                        # )



                #
                # for cl in self._clocks.values():
                #     # todo: how do we know the clock has ended?
                #     signal = cl.update(ts)
                #
                #     if signal:
                #         sts, s_evt, calendar = signal
                #         signal = clock_pb2.Signal(
                #             timestamp=conversions.to_proto_timestamp(sts),
                #             event=evt,
                #             exchange=calendar)
                #
                #         signals.append(signal)

                #
                # signal_handler.update(ts, evt, signals)
                # # process cached commands
                # signal_handler.process(ts)
                # # continue execution
                # signal_handler.clock_update(ts, evt, signals)
                process.clock_update(ts, global_event, signals)

    def execute(self, command):
        with self._execution_lock:
            command(self._get_clocks)

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
