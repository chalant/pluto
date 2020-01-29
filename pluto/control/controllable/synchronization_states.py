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
        return self._aggregate(self._controllable, ts, evt, signals)

    #todo: we only need active exchanges for BAR and TRADE_END events
    @abstractmethod
    def _aggregate(self, controllable, ts, evt, signals):
        raise NotImplementedError

class BFS(State):
    def __init__(self, controllable):
        super(BFS, self).__init__(controllable)
        self._session_end = 0
        self._num_exchanges = len(controllable.exchanges)

    @property
    def name(self):
        return 'bfs'

    def _aggregate(self, controllable, ts, evt, signals):
        exchanges = controllable.exchanges
        active = []
        for t, c_evt, exchange in signals:
            if exchange in exchanges:
                if c_evt == SESSION_END:
                    self._session_end += 1
                    if self._session_end == self._num_exchanges:
                        self._session_end = 0
                        controllable.state = controllable.out_session
                        return ts, SESSION_END, []
                elif c_evt == BAR or  c_evt == TRADE_END:
                    active.append((c_evt, exchange))
        if active:
            return ts, evt, active
        else:
            return

class InSession(State):
    @property
    def name(self):
        return 'in_session'

    def _aggregate(self, controllable, ts, evt, signals):
        exchanges = controllable.exchanges
        active = []
        for t, c_evt, exchange in signals:
            # filter exchanges
            if exchanges.get(exchange):
                # filter active exchanges
                if c_evt == SESSION_START:
                    active.append((c_evt, exchange))
        if active:
            #move to active state, the strategy is ready to start executing
            controllable.state = controllable.active
            return (ts, SESSION_START, active)
        else:
            return

class Active(State):
    @property
    def name(self):
        return 'active'

    def _aggregate(self, controllable, ts, evt, signals):
        exchanges = controllable.exchanges
        active = []
        for t, c_evt, exchange in signals:
            if exchanges.get(exchange):
                #filter active exchanges
                if c_evt == BEFORE_TRADING_START:
                    active.append((c_evt, exchange))
        if active:
            controllable.state = controllable.bfs
            return (ts, BEFORE_TRADING_START, active)
        else:
            return

class OutSession(State):
    @property
    def name(self):
        return 'out_session'

    def _aggregate(self, controllable, ts, evt, signals):
        if evt == SESSION_START:
            exchanges = controllable.exchanges
            active = []
            for t, c_evt, exchange in signals:
                # filter exchanges
                if exchanges.get(exchange):
                    # search for a session start event if it have not be done yet.
                    if c_evt == SESSION_START:
                        # flag if we hit one SESSION_START event
                        active.append((c_evt, exchange))
            if active:
                controllable.state = controllable.active
                return (ts, SESSION_START, active)
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
        return BFS(controllable)
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