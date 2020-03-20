from abc import abstractmethod, ABC

from protos.clock_pb2 import (
    SESSION_START,
    SESSION_END,
    BEFORE_TRADING_START,
    BAR,
    TRADE_END
)

class State(ABC):
    def __init__(self, controllable):
        self._controllable = controllable

    @property
    def name(self):
        raise NotImplementedError

    def aggregate(self, ts, evt, signals):
        # print(self.name)
        return self._aggregate(self._controllable, ts, evt, signals)

    #todo: we only need active exchanges for BAR and TRADE_END events
    @abstractmethod
    def _aggregate(self, controllable, ts, evt, signals):
        raise NotImplementedError

class Trading(State):
    def __init__(self, controllable):
        super(Trading, self).__init__(controllable)
        self._session_end = 0
        self._num_exchanges = len(controllable.calendars)

    @property
    def name(self):
        return 'trading'

    def _aggregate(self, controllable, ts, evt, signals):
        calendars = controllable.calendars
        target = []
        for signal in signals:
            exchange = signal.exchange
            c_evt = signal.event
            if calendars.get(exchange):
                if c_evt == SESSION_END:
                    self._session_end += 1
                    if self._session_end == self._num_exchanges:
                        self._session_end = 0
                        controllable.state = controllable.out_session
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

    def _aggregate(self, controllable, ts, evt, signals):
        calendars = controllable.calendars
        active = []
        for signal in signals:
            # filter exchanges
            exchange = signal.exchange
            c_evt = signal.event
            if calendars.get(exchange):
                # filter active exchanges
                if c_evt == SESSION_START:
                    ts = signal.timestamp
                    active.append((c_evt, exchange))
        if active:
            #move to active state, the strategy is ready to start executing
            controllable.state = controllable.active
            return ts, SESSION_START, active
        else:
            return

class Active(State):
    #waits for the first bfs event
    @property
    def name(self):
        return 'active'

    def _aggregate(self, controllable, ts, evt, signals):
        calendars = controllable.calendars
        active = []
        for signal in signals:
            exchange = signal.exchange
            c_evt = signal.event
            if calendars.get(exchange):
                #filter active exchanges
                if c_evt == BEFORE_TRADING_START:
                    ts = signal.timestamp
                    active.append((c_evt, exchange))
        if active:
            controllable.state = controllable.bfs
            return ts, BEFORE_TRADING_START, active
        else:
            return

class OutSession(State):
    #waits for the first session start event, or moves to in session state if no session start event
    # occurs
    @property
    def name(self):
        return 'out_session'

    def _aggregate(self, controllable, ts, evt, signals):
        if evt == SESSION_START:
            calendars = controllable.calendars
            active = []
            for signal in signals:
                # filter exchanges
                s_evt = signal.event
                exchange = signal.exchange
                if calendars.get(signal.exchange):
                    # search for a session start event if it have not be done yet.
                    if s_evt == SESSION_START:
                        ts = signal.timestamp
                        # flag if we hit one SESSION_START event
                        active.append((s_evt, exchange))
            if active:
                controllable.state = controllable.active
                return ts, SESSION_START, active
            else:
                #no session_start event in signals
                controllable.state = controllable.in_session
                return
        else:
            return

class Idle(State):
    @property
    def name(self):
        return 'idle'

    def _aggregate(self, controllable, ts, evt, signals):
        return


def get_state(name, controllable):
    if name == 'bfs':
        return Trading(controllable)
    elif name == 'in_session':
        return InSession(controllable)
    elif name == 'out_session':
        return OutSession(controllable)
    elif name == 'active':
        return Active(controllable)
    elif name == 'idle':
        return Idle(controllable)
    else:
        return