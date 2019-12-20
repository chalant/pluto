from protos import clock_pb2

import abc
from datetime import datetime, timedelta, time
import pandas as pd

from google.protobuf import timestamp_pb2

from pluto.trading_calendars import calendar_utils as cu

import pluto.control.clock.sim_engine as sim

from trading_calendars.utils.pandas_utils import days_at_time
from datetime import timedelta


class StopExecution(Exception):
    pass


class ClockEvent(object):
    __slots__ = ['dt', 'label', 'event', 'exchange']

    def __init__(self, dt, label, event, exchange):
        self.dt = dt
        self.event = event
        self.exchange = exchange
        self.label = label

    def to_proto(self):
        ts = timestamp_pb2.Timestamp()
        ts.FromDatetime(self.dt)
        return clock_pb2.ClockEvent(
            timestamp=ts,
            event=clock_pb2.ClockEvent,
            exchange_name=self.exchange_name)

class FakeClock(object):
    def update(self, dt):
        return ()

# a clock is responsible of filtering time events from the loop.
class Clock(object):
    def __init__(self, exchange, start_dt, end_dt=None, minute_emission=False):

        self._exchange = exchange
        self._handlers = {}

        self._start_dt = start_dt
        self._end_dt = end_dt

        self._first_call_flag = False

        self._calendar = None
        self._minute_emission = minute_emission

        self._load_attributes(start_dt, end_dt)

        # next evt and dt
        self._nxt_dt, self._nxt_evt = next(self._generator)

        self._sess_idx = 0
        self._stop = False
        self._reload_flag = False if end_dt else True

    @property
    def exchange(self):
        return self._exchange

    @property
    def all_sessions(self):
        return self._calendar.all_sessions

    @property
    def first_session(self):
        return self.all_sessions[0]

    @property
    def last_session(self):
        return self.all_sessions[-1]

    def add_signal_handler(self, handler):
        self._handlers[handler] = handler

    def get_sessions(self, length):
        sess_idx = self._sess_idx
        #returns a slice of sessions of some length
        return self._calendar.all_sessions[sess_idx: sess_idx + length]

    def minutes_for_session(self, session_label):
        return self._calendar.minutes_for_session(session_label)

    def execution_minutes_for_session(self, session_label):
        return self._calendar.execution_minutes_for_session(session_label)

    def get_generator(self, sessions):
        # loops every x frequency
        calendar = self._calendar

        trading_o_and_c = calendar.schedule.loc[sessions]
        market_closes = trading_o_and_c['market_close']



        minute_emission = self._minute_emission

        if minute_emission:
            market_opens = trading_o_and_c['market_open']
            execution_opens = calendar.execution_time_from_open(market_opens)
            execution_closes = calendar.execution_time_from_close(market_closes)
        else:
            execution_closes = calendar.execution_time_from_close(market_closes)
            execution_opens = execution_closes

        return sim.MinuteSimulationClock(
            sessions,
            execution_opens,
            execution_closes,
            pd.DatetimeIndex([ts - pd.Timedelta(minutes=2) for ts in execution_opens]),
            minute_emission)

    def update(self, dt):
        # todo: consider using a finite state machine for clocks
        # todo: should we send the current dt or pass the datetime at the moment of transfer?
        #  should we send both the current dt and the expected dt?
        # todo: we should consider reloading when there is a calendar update...
        # todo: should we catch a stop iteration?
        ts, evt = self._nxt_dt, self._nxt_evt
        # if the generator is lagging behind, synchronize it with the current dt
        # the ts must be either bigger or equal to dt.

        # print('{} Expected {} {} got {}'.format(self.exchange, ts, evt, dt))

        if ts < dt:
            while ts < dt:
                # will stop if it is equal or bigger than dt
                # todo: we also need to update the sess_idx, since the we might change sessions
                ts, evt = self._next_(dt)
                # output_.append(ts)
                if evt == clock_pb2.SESSION_START:
                    self._sess_idx += 1
            # print('{} Synchronized to {} {}'.format(self.exchange, ts, evt))
            self._nxt_dt, self._nxt_evt = ts, evt

        if dt == ts:
            # output_.append(dt)
            # print(self._calendar.name, dt)
            # advance if the timestamp matches.
            self._nxt_dt, self._nxt_evt = nxt_ts, nxt_evt = self._next_(dt)

            # todo: it the real_dt is between session start and bfs, send a session start
            # (and initialize if it's a first call). actually, the initialize could be handled
            # client side.
            # if the dt is between the session

            # if it has not been initialized yet.
            if not self._first_call_flag:
                # only consider session start events
                # todo: problem with the before trading starts event...
                # todo: this will never happen... we will always have time between sess start
                #  and bfs
                if evt == clock_pb2.SESSION_START and nxt_evt == clock_pb2.BEFORE_TRADING_START:
                    # we can still initialize if we're between the session start ts and
                    # bfs ts
                    if dt < nxt_ts:
                        # self._notify(real_dt, dt, evt)
                        self._first_call_flag = True
                    self._sess_idx += 1
                    # print('{} Session start {}'.format(self.exchange, ts))
                # we just pass all the other events
                elif evt == clock_pb2.BAR:
                    minute_emission = self._minute_emission
                    if minute_emission:
                        # skip the minute_end event
                        self._nxt_dt, self._nxt_evt = nxt_ts, nxt_evt = self._next_(dt)
                        if nxt_evt == clock_pb2.SESSION_END:
                            # skip the session_end event
                            # print('{} Session end {}'.format(self.exchange, nxt_ts))
                            self._nxt_dt, self._nxt_evt = self._next_(dt)
                    else:
                        # skip the session_end event
                        self._nxt_dt, self._nxt_evt = self._next_(dt)
            else:
                minute_emission = self._minute_emission
                if evt == clock_pb2.BAR:
                    # the session end event comes after the bar event
                    if minute_emission:
                        # call next once again for the minute end event
                        # minute_event
                        # session_end
                        self._nxt_dt, self._nxt_evt = nxt_ts, nxt_evt = self._next_(dt)
                        # call next once again for the minute end event
                        if nxt_evt == clock_pb2.SESSION_END:
                            # print('{} Session end {}'.format(self.exchange, nxt_ts))
                            self._nxt_dt, self._nxt_evt = self._next_(dt)
                    else:
                        # get the session_start event
                        self._nxt_dt, self._nxt_evt = self._next_(dt)
                elif evt == clock_pb2.SESSION_START:
                    # update the session idx
                    # print('{} Session start {}'.format(self.exchange, ts))
                    self._sess_idx += 1
                elif evt == clock_pb2.SESSION_END:
                    # print('{} Session end {}'.format(self.exchange, nxt_ts))
                    self._nxt_dt, self._nxt_evt = self._next_(dt)
            return ts, evt, self._exchange
        else:
            #timestamp is greater, we sont consider the signal
            return ()


    def _next_(self, dt):
        try:
            return next(self._generator)
        except StopIteration:
            if dt < self._end_dt:
                self._load_attributes(pd.Timestamp(pd.Timestamp.combine(dt + pd.Timedelta('1 day'), time.min), tz='UTC'))
                # self._notify(real_dt, dt, clock_pb2.CALENDAR)
                self._sess_idx = 0
                # print('Current timestamp {} Last timestamp {}'.format(dt, self._end_dt))
                return  next(self._generator)
            else:
                #we've reached the end date, reload only if the clock runs forever
                if self._reload_flag:
                    self._load_attributes(
                        pd.Timestamp(pd.Timestamp.combine(dt + pd.Timedelta('1 day'), time.min), tz='UTC'))
                    # self._notify(real_dt, dt, clock_pb2.CALENDAR)
                    self._sess_idx = 0
                    # print('Current timestamp {} Last timestamp {}'.format(dt, self._end_dt))
                    return next(self._generator)
                else:
                    raise StopExecution


    def _load_attributes(self, start_dt, end_dt=None):
        self._start_dt = start_dt

        self._calendar = cal = cu.get_calendar_in_range(self.exchange, start_dt, end_dt)

        self._end_dt = end_dt if end_dt else cal.all_sessions[-1]

        self._generator = iter(self.get_generator(cal.all_sessions))


class ClockSignalRouter(abc.ABC):
    def __init__(self):
        self._exg_listeners = {}
        self._sess_per_exg = {}

    def on_clock_event(self, request, context):
        request = self._on_clock_event(request)
        for listener in self._exg_listeners[request.exchange_name]:
            listener.update(request)

    def get_clock(self, exchange):
        # returns a clock stub
        return self._get_clock(exchange)

    def register_listener(self, clock):
        # returns a clock client (stub)
        listener = self._get_listener()
        exl = self._exg_listeners
        exchange = clock.exchange
        l = exl.get(exchange, None)
        if not l:
            exl[exchange] = [listener]
            self._num_clocks += 1
        else:
            l.append(l)
        return listener

    @abc.abstractmethod
    def _on_clock_event(self, request):
        raise NotImplementedError

    @abc.abstractmethod
    def _get_listener(self):
        raise NotImplementedError

    @abc.abstractmethod
    def _get_clock(self, exchange):
        raise NotImplementedError


class ClockListener(abc.ABC):
    @abc.abstractmethod
    def clock_update(self, request):
        raise NotImplementedError

    @abc.abstractmethod
    def register_session(self, session):
        raise NotImplementedError


class BaseClockListener(ClockListener):
    def __init__(self):
        self._sessions = {}

    def clock_update(self, request):
        # for performing additional stuff before calling the sessions
        self._clock_update(request, self._sessions.values())

    def register_session(self, session):
        self._session[session.id] = session

    @abc.abstractmethod
    def _clock_update(self, request, sessions):
        raise NotImplementedError


class SignalFilter(object):
    def __init__(self):
        self._update_fn = self._pass

    def clock_update(self, clock_evt):
        self._clock_update(clock_evt)

    def _pass(self):
        pass

    @abc.abstractmethod
    def _clock_update(self, clock_evt):
        raise NotImplementedError

    @abc.abstractmethod
    def add_session(self, session):
        raise NotImplementedError

    def activate(self):
        self._update_fn = self.clock_update

    def deactivate(self):
        self._update_fn = self._pass


class BaseSignalFilter(SignalFilter):
    def __init__(self):
        self._sessions = []

    def _clock_update(self, clock_evt):
        for session in self._sessions:
            session.update(clock_evt)

    def add_session(self, session):
        self._sessions.append(session)

class LiveSignalFilter(SignalFilter):
    def __init__(self, downloader):
        self._sessions = []

    def _clock_update(self, clock_evt):
        pass


class HeadSignalFilterDecorator(SignalFilter):
    def __init__(self, signal_filter):
        '''
        Parameters
        ----------
        signal_filter : SignalFilter
        '''
        self._signal_filter = signal_filter

    def _clock_update(self, clock_evt):
        self._signal_filter.clock_update(self._dec_clock_update(clock_evt))

    @abc.abstractmethod
    def _dec_clock_update(self, clock_evt):
        raise NotImplementedError

    def add_session(self, session):
        self._signal_filter.add_session(session)

    @abc.abstractmethod
    def _clock_update(self, clock_evt):
        raise NotImplementedError


class TailSignalFilterDecorator(SignalFilter):
    def __init__(self, signal_filter):
        self._signal_filter = signal_filter

    def _clock_update(self, clock_evt):
        self._signal_filter.update(clock_evt)
        self._dec_clock_update(clock_evt)

    @abc.abstractmethod
    def _dec_clock_update(self, clock_evt):
        raise NotImplementedError


class DelimitedSignalFilter(HeadSignalFilterDecorator):
    def __init__(self, signal_filter, start_dt, end_dt):
        super(DelimitedSignalFilter, self).__init__(signal_filter)

        self._start_date = start_dt
        self._end_date = end_dt
        self._sessions = []
        self._first_call = False

    def _dec_clock_update(self, clock_evt):
        # filter the signals depending on the timestamp
        dt = clock_evt.ts
        if dt == self._end_date:
            # todo: what about liquidation? => liquidation should be done on the broker
            # all the controllables will be updated by retrieving the broker state or
            # we signal all the controllables that a liquidation has occured
            clock_evt.event = clock_pb2.COMPLETED
        elif dt == self._start_date:
            if not self._first_call:
                clock_evt.event = clock_pb2.INITIALIZE
                self._first_call = True
            else:
                clock_evt.event = clock_pb2.SESSION_START


class CallBackSignalFilter(TailSignalFilterDecorator):
    def __init__(self, signal_filter, callback):
        super(CallBackSignalFilter, self).__init__(signal_filter)
        self._callback = callback

    def _dec_clock_update(self, clock_evt):
        self._clock_listener.update(clock_evt)
        self._callback(clock_evt)

    def register_session(self, session):
        return self._clock_listener.register_session(session)
