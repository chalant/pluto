from protos import clock_pb2

import abc
from datetime import datetime, timedelta
import pandas as pd

from google.protobuf import timestamp_pb2

from contrib.control.clock import minute_event_gen as meg
from contrib.trading_calendars import calendar_utils as cu

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
            event =clock_pb2.ClockEvent,
            exchange_name=self.exchange_name)

class Clock(abc.ABC):
    def __init__(self, exchange):
        self._exchange = exchange
        self._handlers = []

    def update(self, real_dt, dt):
        #todo: should we adjust the real datetime with the delay?
        evt = self._get_dt_evt(dt)
        if evt:
            for handler in self._handlers:
                handler.update(ClockEvent(real_dt, evt, self._exchange))


    @abc.abstractmethod
    def _get_dt_evt(self, dt):
        raise NotImplementedError

    def add_signal_filter(self, filter):
        self._handlers.append(filter)

class MinuteClock(Clock):
    def __init__(self, calendar, start_dt, end_dt, bfs_offset=None, minute_emission=False):
        '''

        Parameters
        ----------
        calendar : trading_calendars.TradingCalendar
        start_dt
        end_dt
        minute_emission
        '''
        self._start_dt = start_dt
        self._end_dt = end_dt

        self._first_call_flag = False
        self._calendar = calendar

        self._time_idx = -1

        self._load_attributes(start_dt, calendar, minute_emission)
        self._minute_emission = minute_emission

        dt = datetime.combine(datetime.min, self.open_time)
        if not bfs_offset:
            self._offset = offset = timedelta(minutes=15)
        else:
            self._offset = offset = bfs_offset
        dt = dt - offset
        self._bfs = dt.time()

    def _load_proto_calendar(self):
        return

    @property
    def bfs_offset(self):
        self._calendar.next_session_label()
        return self._offset


    @property
    def open_time(self):
        self._calendar.open_times[0]

    def session_open(self, session_label):
        return self._calendar.session_open(session_label)

    @property
    def all_sessions(self):
        self._calendar.all_sessions

    @property
    def first_session(self):
        return self.all_sessions[0]

    @property
    def last_session(self):
        return self.all_sessions[-1]

    @property
    def all_minutes(self):
        cal = self._calendar
        for dt, evt in meg.get_event_generator(
            cal,
            cal.all_sessions[0],
            cal.all_sessions[-1],
            self._bfs,
            minute_emission=self._minute_emission):
            yield dt

    def minutes_in_range(self, start_session, end_session):
        for dt, evt in meg.get_event_generator(
            self._calendar,
            start_session,
            end_session,
            self._bfs,
            minute_emission=self._minute_emission):
            yield dt

    def reset(self, start_dt, end_dt):
        self._start_dt = start_dt
        self._end_dt = end_dt

    def _update(self, dt, event):
        for handler in self._handlers:
            handler.update(ClockEvent(dt, event, self._exchange))

    def _get_dt_evt(self, real_dt, dt):
        #todo: check for a stop event
        #todo: in minute emission, we need to send the bar event, the minute end event
        #todo: should we send the current dt? should we send both the current dt and the expected dt?
        if not self._stop:
            try:
                ts, evt = next(self._generator)
                if dt == ts:
                    if not self._first_call_flag:
                        self._first_call_flag = True
                        self._update(real_dt, clock_pb2.INITIALIZE)
                    else:
                        #an event is triggered right after a "BAR" event (either a MINUTE_END or a SESSION_END)
                        #so we call next one more time.
                        if evt == clock_pb2.BAR:
                            try:
                                ts, evt = next(self._generator)
                                #the session end event comes after the bar event
                                if evt == clock_pb2.SESSION_END:
                                    #todo: we probably don't need to reload the clocks here...
                                    # sleep until next session start
                                    end_dt = self._end_dt
                                    #todo: reload calendar once the end date is reached
                                    #todo: do we need the calendar event? => yes: elements
                                    # server side, like DataPortal needs.
                                    if dt.date() == end_dt.date():
                                        # re-load all attributes
                                        self._load_attributes(
                                            end_dt + pd.Timedelta('1 day'),
                                            self._load_proto_calendar(),
                                            self._minute_emission)
                                        # yield a calendar event so that the clients may load a new calendar
                                        self._update(real_dt, clock_pb2.CALENDAR)
                                    self._update(real_dt, evt)
                                else:
                                    self._update(real_dt, evt)
                            except StopIteration:
                                self._update(real_dt, clock_pb2.STOP)
            except StopIteration:
                self._update(real_dt, clock_pb2.STOP)
        else:
            self._update(real_dt, clock_pb2.STOP)

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
