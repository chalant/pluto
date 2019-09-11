from protos import clock_pb2

import abc
from datetime import datetime, timedelta
import pandas as pd

from google.protobuf import timestamp_pb2

from contrib.trading_calendars import calendar_utils as cu

import contrib.control.clock.sim_engine as sim

from trading_calendars.utils.pandas_utils import days_at_time


class ClockEvent(object):
    __slots__ = ['dt', 'event', 'exchange_name']

    def __init__(self, dt, event, exchange_name):
        self.dt = dt
        self.event = event
        self.exchange_name = exchange_name

    def to_proto(self):
        ts = timestamp_pb2.Timestamp()
        ts.FromDatetime(self.dt)
        return clock_pb2.ClockEvent(
            timestamp=ts,
            event=clock_pb2.ClockEvent,
            exchange_name=self.exchange_name)


# a clock is responsible of filtering time events from the loop.
class Clock(object):
    #todo:
    def __init__(self, exchange, start_dt, end_dt=None, minute_emission=False):
        '''

        Parameters
        ----------
        calendar : trading_calendars.TradingCalendar
        minute_emission
        '''

        self._exchange = exchange
        self._handlers = []

        self._start_dt = start_dt
        self._end_dt = end_dt

        self._first_call_flag = False

        self._calendar = None
        self._load_attributes(start_dt, minute_emission)

        self._minute_emission = minute_emission

        dt = datetime.combine(datetime.min, self._calendar.open_times[0][1])
        dt = dt - offset
        self._bfs = dt.time()

        # next evt and dt
        self._nxt_dt, self._nxt_evt = next(self._generator)

        self._sess_idx = 0
        self._stop = False
        self._reload = True if not end_dt else False

    @property
    def exchange(self):
        return self._exchange

    @property
    def open_time(self):
        self._calendar.open_times[0][1]

    def session_open(self, session_label):
        return self._calendar.session_open(session_label)

    @property
    def all_sessions(self):
        return self._calendar.all_sessions

    @property
    def first_session(self):
        return self.all_sessions[0]

    @property
    def last_session(self):
        return self.all_sessions[-1]

    def add_signal_filter(self, filter):
        self._handlers.append(filter)

    def get_sessions(self, length):
        sess_idx = self._sess_idx
        #returns a slice of sessions of some length
        return self._calendar.all_sessions[sess_idx: sess_idx + length]

    def reset(self):
        #todo: we need to move back to first start dt and first end dt as-well...
        self._sess_idx = 0

    def get_generator(self, sessions):
        # loops every x frequency
        calendar = self._calendar

        trading_o_and_c = calendar.schedule.loc[sessions]
        market_closes = trading_o_and_c['market_close']

        bts_minutes = days_at_time(
            sessions,
            self._bfs,
            calendar.tz
        )

        minute_emission = self._minute_emission

        if minute_emission:
            market_opens = trading_o_and_c['market_open']
            execution_opens = calendar.execution_time_from_open(market_opens)
            execution_closes = calendar.execution_time_from_close(market_closes)
        else:
            execution_closes = calendar.execution_time_from_close(market_closes)
            execution_opens = execution_closes

        return sim.MinuteSimulationClock(sessions, execution_opens, execution_closes, bts_minutes, minute_emission)

    def update(self, real_dt, dt):
        # todo: consider using a finite state machine for clocks
        # todo: should we send the current dt or pass the datetime at the moment of transfer?
        #  should we send both the current dt and the expected dt?
        if not self._stop:
            # todo: should we catch a stop iteration?
            ts, evt = self._nxt_dt, self._nxt_evt
            # if the generator is lagging behind, synchronize it with the current dt
            # the ts must be either bigger or equal to dt.

            print('{} Expected {} {} got {}'.format(self.exchange, ts, evt, dt))

            if ts < dt:
                while ts < dt:
                    # will stop if it is equal or bigger than dt
                    # todo: we also need to update the sess_idx, since the we might change sessions
                    ts, evt = next(self._generator)
                    if evt == clock_pb2.SESSION_START:
                        self._sess_idx += 1
                print('{} Synchronized to {} {}'.format(self.exchange, ts, evt))
                self._nxt_dt, self._nxt_evt = ts, evt

            if dt == ts:
                # print(self._calendar.name, dt)
                # advance if the timestamp matches.
                self._nxt_dt, self._nxt_evt = nxt_ts, nxt_evt = next(self._generator)

                # todo: it the real_dt is between session start and bfs, send a session start
                # (and initialize if it's a first call). actually, the initialize could be handled
                # client side.
                # if the dt is between the session

                # if it has not been initialized yet.
                if not self._first_call_flag:
                    # only consider session start events
                    # todo: problem with the before trading starts event...
                    if evt == clock_pb2.SESSION_START and nxt_evt == clock_pb2.BEFORE_TRADING_START:
                        # we can still initialize if we're between the session start ts and
                        # bfs ts
                        if dt < nxt_ts:
                            self._notify(real_dt, evt)
                            self._first_call_flag = True
                        self._sess_idx += 1
                    # we just pass all the other events
                    elif evt == clock_pb2.BAR:
                        minute_emission = self._minute_emission
                        if minute_emission:
                            # skip the minute_end event
                            self._nxt_dt, self._nxt_evt = nxt_ts, nxt_evt = next(self._generator)
                            if nxt_evt == clock_pb2.SESSION_END:
                                # skip the session_end event
                                self._nxt_dt, self._nxt_evt = next(self._generator)
                        else:
                            # skip the session_end event
                            self._nxt_dt, self._nxt_evt = next(self._generator)

                else:
                    minute_emission = self._minute_emission
                    if evt == clock_pb2.BAR:
                        self._notify(real_dt, evt)
                        # the session end event comes after the bar event
                        if minute_emission:
                            # call next once again for the minute end event
                            # minute_event
                            self._notify(real_dt, nxt_evt)
                            # session_end
                            self._nxt_dt, self._nxt_evt = nxt_ts, nxt_evt = next(self._generator)
                            # call next once again for the minute end event
                            if nxt_evt == clock_pb2.SESSION_END:
                                self._reload(real_dt, minute_emission, nxt_ts)
                                self._notify(real_dt, nxt_evt)
                                self._nxt_dt, self._nxt_evt = next(self._generator)
                        else:
                            self._reload(real_dt, minute_emission, nxt_ts)
                            self._notify(real_dt, nxt_evt)
                            # session_start event
                            self._nxt_dt, self._nxt_evt = next(self._generator)
                    elif evt == clock_pb2.SESSION_START:
                        # update the session idx
                        self._sess_idx += 1
                        self._notify(real_dt, evt)
                    elif evt == clock_pb2.SESSION_END:
                        self._nxt_dt, self._nxt_evt = next(self._generator)
        else:
            self._notify(real_dt, clock_pb2.STOP)

    def _notify(self, dt, event):
        exchange = self._exchange
        for handler in self._handlers:
            handler.update(ClockEvent(dt, event, exchange))

    def _reload(self, real_dt, minute_emission, ts):
        if self._reload:
            # it's a session_end event
            end_dt = self._end_dt
            # todo: reload calendar once the end date is reached
            # todo: do we need the calendar event? => yes: elements
            # server side, like DataPortal needs the calendar.
            # on session end, it the date is the same as the end,
            # reload the calendar, with a new window
            # todo: we assume that the start_dt and the end_dt and the shift
            # are the same as the loop.
            if ts.date() == end_dt.date():
                # re-load all attributes
                self._load_attributes(end_dt + pd.Timedelta('1 day'))
                # send a calendar event so that the clients may load a new calendar
                self._notify(real_dt, clock_pb2.CALENDAR)

    def _load_attributes(self, start_dt, end_dt=None):
        self._start_dt = start_dt

        self._calendar = cal = cu.get_calendar_in_range(start_dt, end_dt)

        self._end_dt = end_dt if end_dt else cal.all_sessions[-1]

        self._generator = iter(self.get_generator(cal.all_sessions))


class ClockSignalRouter(abc.ABC):
    def __init__(self):
        self._exg_listeners = {}
        self._sess_per_exg = {}

    def on_clock_event(self, request, context):
        request = self._on_clock_event(request)
        for listener in self._exg_listeners[request.exchange_name]:
            listener.clock_update(request)

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
        self._update_fn(clock_evt)

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
            session.clock_update(clock_evt)

    def add_session(self, session):
        self._sessions.append(session)


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
        self._signal_filter.clock_update(clock_evt)
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
        self._clock_listener.clock_update(clock_evt)
        self._callback(clock_evt)

    def register_session(self, session):
        return self._clock_listener.register_session(session)
