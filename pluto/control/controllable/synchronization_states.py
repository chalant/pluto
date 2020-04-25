from abc import abstractmethod, ABC

from protos.clock_pb2 import (
    SESSION_START,
    SESSION_END,
    BEFORE_TRADING_START,
    BAR,
    TRADE_END
)

class State(ABC):
    def __init__(self, tracker):
        self._tracker = tracker

    @property
    def name(self):
        raise NotImplementedError

    def aggregate(self, ts, evt, signals):
        # print(self.name)
        return self._aggregate(self._tracker, ts, evt, signals)

    #todo: we only need active exchanges for BAR and TRADE_END events
    @abstractmethod
    def _aggregate(self, tracker, ts, evt, signals):
        raise NotImplementedError

class Trading(State):
    def __init__(self, tracker):
        super(Trading, self).__init__(tracker)
        self._session_end = 0
        self._num_calendars = len(tracker.exchanges)

    @property
    def name(self):
        return 'trading'

    def _aggregate(self, tracker, ts, evt, signals):
        exchanges = tracker.exchanges
        target = []
        for signal in signals:
            exchange = signal.exchange
            c_evt = signal.event
            if exchanges.get(exchange):
                if c_evt == SESSION_END:
                    self._session_end += 1
                    if self._session_end == self._num_calendars:
                        self._session_end = 0
                        tracker.state = tracker.out_session
                        target.append((c_evt, exchange))
                        ts = signal.timestamp
                        # return signal.timestamp, SESSION_END, []
                elif c_evt == BAR or c_evt == TRADE_END:
                    ts = signal.timestamp
                    target.append((c_evt, exchange))
        if target:
            return ts, evt, target
        else:
            return

class InSession(State):
    #waits for the first session start event
    @property
    def name(self):
        return 'in_session'

    def _aggregate(self, tracker, ts, evt, signals):
        exchanges = tracker.exchanges
        active = []
        for signal in signals:
            # filter exchanges
            exchange = signal.exchange
            c_evt = signal.event
            if exchanges.get(exchange):
                # filter active exchanges
                if c_evt == SESSION_START:
                    ts = signal.timestamp
                    active.append((c_evt, exchange))
        if active:
            #move to active state, the strategy is ready to start executing
            tracker.state = tracker.active
            return ts, SESSION_START, active
        else:
            return

class Active(State):
    #waits for the first bfs event
    @property
    def name(self):
        return 'active'

    def _aggregate(self, tracker, ts, evt, signals):
        exchanges = tracker.exchanges
        active = []
        for signal in signals:
            exchange = signal.exchange
            c_evt = signal.event
            if exchanges.get(exchange):
                #filter active exchanges
                if c_evt == BEFORE_TRADING_START:
                    ts = signal.timestamp
                    active.append((c_evt, exchange))
        if active:
            tracker.state = tracker.bfs
            return ts, BEFORE_TRADING_START, active
        else:
            return

class OutSession(State):
    #waits for the first session start event, or moves to in session state if no session start event
    # occurs
    @property
    def name(self):
        return 'out_session'

    def _aggregate(self, tracker, ts, evt, signals):
        if evt == SESSION_START:
            exchanges = tracker.exchanges
            active = []
            for signal in signals:
                # filter exchanges
                s_evt = signal.event
                exchange = signal.exchange
                if exchanges.get(signal.exchange):
                    # search for a session start event if it have not be done yet.
                    if s_evt == SESSION_START:
                        ts = signal.timestamp
                        # flag if we hit one SESSION_START event
                        active.append((s_evt, exchange))
            if active:
                tracker.state = tracker.active
                return ts, SESSION_START, active
            else:
                #no session_start event in signals
                tracker.state = tracker.in_session
                return
        else:
            return

class Idle(State):
    @property
    def name(self):
        return 'idle'

    def _aggregate(self, tracker, ts, evt, signals):
        return

class Tracker(object):
    def __init__(self, calendars):
        self._state = None

        self._calendars = {calendar:calendar for calendar in calendars}

        self._bfs = Trading(self)
        self._out_session = OutSession(self)
        self._in_session = InSession(self)
        self._active = Active(self)
        self._idle = idle = Idle(self)
        self._state = idle
    @property
    def out_session(self):
        return self._out_session

    @property
    def in_session(self):
        return self._in_session

    @property
    def active(self):
        return self._active

    @property
    def idle(self):
        return self._idle

    @property
    def trading(self):
        return self._bfs

    @property
    def calendars(self):
        return self._calendars

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, value):
        self._state = value

    def aggregate(self, dt, evt, signals):
        self._state.aggregate(dt, evt, signals)

def set_state(name, tracker):
    if name == 'bfs':
        tracker.state = tracker.trading
    elif name == 'in_session':
        tracker.state = tracker.in_session
    elif name == 'out_session':
        tracker.state = tracker.out_session
    elif name == 'active':
        tracker.state = tracker.active
    elif name == 'idle':
        tracker.state = tracker.idle
    else:
        return