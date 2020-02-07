import collections
import threading

from pluto.coms.utils import conversions
from pluto.control.clock import clock
from pluto.trading_calendars import calendar_utils as cu
from pluto.control.clock.utils import get_generator

from protos import clock_pb2


class MinuteSimulationLoop(object):
    def __init__(self, control_mode, start_dt, end_dt):

        self._start_dt = start_dt
        self._end_dt = end_dt

        self._clocks = clocks = {}
        # create fake clock
        clocks['fake'] = clock.FakeClock()

        self._calendar = cu.get_calendar_in_range('24/7', start_dt, end_dt)
        self._num_clocks = len(clocks)

        self._init_flag = False
        self._start_flag = False
        self._bfs_flag = False

        self._control_mode = control_mode

        self._execution_lock = threading.Lock()
        self._to_execute = collections.deque()

    def start(self):
        calendar = self._calendar

        control_mode = self._control_mode

        for ts, evt in get_generator(calendar, calendar.all_sessions):
            # acquire lock so that no further commands are executed here
            # while this block is being executed
            with self._execution_lock:
                # process any cached values
                self._process(ts)
                #call for any update that needs to be done before
                #anythin else
                control_mode.update(ts, evt)
                # process cached commands
                control_mode.process(ts)
                signals = []
                # aggregate the signals into a single signal.
                for cl in self._clocks.values():
                    signal = cl.update(ts)
                    if signal:
                        sts, s_evt, exchange = signal
                        signal = clock_pb2.Signal(
                            timestamp=conversions.to_proto_timestamp(sts),
                            event=evt,
                            exchange=exchange)
                        signals.append(signal)
                # continue execution
                control_mode.clock_update(ts, evt, signals)

    def execute(self, command):
        with self._execution_lock:
            command(self._control_mode, self._get_clocks)

    def stop(self):
        pass

    def _create_clock(self, exchange):
        return clock.Clock(
            exchange,
            self._start_dt,
            self._end_dt,
            minute_emission=True)

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
