import collections
import threading

from pluto.coms.utils import conversions
from pluto.control.clock import clock
from pluto.trading_calendars import calendar_utils as cu
from pluto.control.clock.utils import get_generator

from protos import clock_pb2


class SimulationLoop(object):
    def __init__(self, start_dt, end_dt, frequency='day', max_reloads=0):

        self._start_dt = start_dt
        self._end_dt = end_dt

        self._frequency = frequency
        self._max_reloads = max_reloads

        self._clocks = clocks = {}
        # create fake clock
        clocks['fake'] = clock.FakeClock()

        self._calendar = cu.get_calendar_in_range('24/7', start_dt, end_dt)
        self._num_clocks = len(clocks)

        self._init_flag = False
        self._start_flag = False
        self._bfs_flag = False

        self._control_modes = []

        self._execution_lock = threading.Lock()
        self._to_execute = collections.deque()

    def start(self):
        calendar = self._calendar

        control_modes = self._control_modes

        for ts, evt in get_generator(calendar, calendar.all_sessions, frequency=self._frequency):
            # acquire lock so that no further commands are executed here
            # while this block is being executed
            with self._execution_lock:
                # process any cached values
                self._process(ts)
                #call for any update that needs to be done before
                #anythin else
                
                signals = []
                # aggregate the signals into a single signal.
                for cl in self._clocks.values():
                    #todo: how do we know the clock has ended?
                    #todo:
                    signal = cl.update(ts)
                    if signal:
                        sts, s_evt, exchange = signal
                        signal = clock_pb2.Signal(
                            timestamp=conversions.to_proto_timestamp(sts),
                            event=evt,
                            exchange=exchange)
                        signals.append(signal)
                
                #
                for control_mode in control_modes:
                    control_mode.update(ts, evt, signals)
                    # process cached commands
                    control_mode.process(ts)
                    # continue execution
                    control_mode.clock_update(ts, evt, signals)

    def execute(self, command):
        with self._execution_lock:
            command(self._get_clocks)

    def add_control_mode(self, mode):
        mode.accept_loop(self)
        self._control_modes.append(mode)

    def stop(self):
        pass

    def _create_clock(self, exchange):
        return clock.Clock(
            exchange,
            self._start_dt,
            self._end_dt,
            minute_emission=True,
            max_reloads=self._max_reloads)

    def _get_clocks(self, exchanges):
        clocks = self._clocks
        for exchange in exchanges:
            exchange = cu.resolve_alias(exchange)
            cl = clocks.get(exchange, None)
            if not cl:
                # create clock, put it in the queue and return it.
                # the clock will be activated on the next loop iteration
                cl = self._create_clock(exchange)
                clocks[exchange] = cl
        try:
            clocks.pop('fake')
        except KeyError:
            pass

    def _process(self, ts):
        pass
