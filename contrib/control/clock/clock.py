from protos import clock_pb2_grpc as cl_grpc
from protos import clock_pb2

import abc

class ClockSignalRouter(cl_grpc.ClockListenerServicer, abc.ABC):
    def __init__(self):
        self._exg_listeners = {}
        self._sess_per_exg = {}

    def OnClockEvent(self, request, context):
        request = self._on_clock_event(request)
        for listener in self._exg_listeners[request.exchange_name]:
            listener.clock_update(request)

    def get_clock(self, exchange):
        #returns a clock stub
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


class DelimitedClockListener(ClockListener):
    def __init__(self, clock_listener, start_date, end_date):
        '''

        Parameters
        ----------
        clock_listener : ClockListener
        start_date
        end_date
        '''
        super(DelimitedClockListener, self).__init__()
        self._clock_listener = clock_listener
        self._start_date = start_date
        self._end_date = end_date
        self._first_call = False

    def clock_update(self, request):
        #filter the signals depending on the timestamp
        dt = request.timestamp.ToDatetime()
        if dt == self._end_date:
            # todo: what about liquidation?
            request.event = clock_pb2.STOP
        elif dt == self._start_date:
            if not self._first_call:
                request.event = clock_pb2.INITIALIZE
                self._first_call = True
            else:
                request.event = clock_pb2.SESSION_START
        self._clock_listener.clock_update(request)

    def register_session(self, session):
        return self._clock_listener.register_session(session)

class CallBackClockListener(ClockListener):
    def __init__(self, clock_listener, callback):
        self._clock_listener = clock_listener
        self._callback = callback

    def clock_update(self, request):
        self._clock_listener.clock_update(request)
        self._callback(request)

    def register_session(self, session):
        return self._clock_listener.register_session(session)