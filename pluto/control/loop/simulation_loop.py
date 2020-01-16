import datetime
import collections
import threading

import pandas as pd


from pluto.control.clock import clock
from pluto.trading_calendars import calendar_utils as cu
from pluto.control.clock import sim_engine as sim
from pluto.control.modes import simulation_mode


def get_generator(calendar, sessions, minute_emission=False, frequency='day'):
    # loops every x frequency

    trading_o_and_c = calendar.schedule.loc[sessions]
    market_closes = trading_o_and_c['market_close']

    if minute_emission:
        market_opens = trading_o_and_c['market_open']
        execution_opens = calendar.execution_time_from_open(market_opens)
        execution_closes = calendar.execution_time_from_close(market_closes)
    else:
        execution_closes = calendar.execution_time_from_close(market_closes)
        execution_opens = execution_closes

    if frequency == 'minute':
        return sim.MinuteSimulationClock(
            sessions,
            execution_opens,
            execution_closes,
            pd.DatetimeIndex([ts - pd.Timedelta(minutes=2) for ts in execution_opens]),
            pd.DatetimeIndex([ts - pd.Timedelta(minutes=15) for ts in execution_closes]),
            minute_emission)
    else:
        return sim.MinuteSimulationClock(
            sessions,
            execution_closes,
            execution_closes,
            pd.DatetimeIndex([ts - pd.Timedelta(minutes=2) for ts in execution_opens]),
            execution_closes,
            minute_emission
        )


class MinuteSimulationLoop(object):
    def __init__(self, control_mode, start_dt, end_dt):

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
            #acquire lock so that no further commands are executed here
            #while this block is being executed
            with self._execution_lock:
                #process any cached values
                self._process(ts)
                signals = []
                # aggregate the signals into a single signal.
                for cl in self._clocks.values():
                    signal = cl.update(ts)
                    if signal:
                        ts, c_evt, exchange = signal
                        signals.append(signal)

                #processes all cached commands
                control_mode.process()
                # update the mode
                control_mode.clock_update(ts, evt, signals)

    def execute(self, command):
        with self._execution_lock:
            command(self._control_mode, self._get_clocks)

    def stop(self):
        pass

    def _create_clock(self, exchange):
        return clock.Clock(
            exchange,
            # todo: if the current date time is above the open time, we need to move to the next session.
            pd.Timestamp(pd.Timestamp.combine(pd.Timestamp.utcnow(), datetime.time.min), tz='UTC'),
            minute_emission=True)

    def _get_clocks(self, exchanges):
        clocks = self._clocks
        for exchange in exchanges:
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
