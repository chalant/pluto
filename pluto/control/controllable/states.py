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
    def _aggregate(self, controllable, ts, evt, signals):
        exchanges = controllable.exchanges
        active = []
        for t, c_evt, exchange in signals:
            # filter exchanges
            if exchanges.get(exchange):
                # search for a session start event if it have not be done yet.
                if c_evt == SESSION_START:
                    active.append((c_evt, exchange))
        if active:
            controllable.state = controllable.active
            return (ts, SESSION_START, active)
        else:
            return

class Active(State):
    def _aggregate(self, controllable, ts, evt, signals):
        exchanges = controllable.exchanges
        active = []
        for t, c_evt, exchange in signals:
            # filter exchanges
            # todo: map exchange to itself so that we can use get() => faster
            if exchange in exchanges:
                # search for a session start event if it have not be done yet.
                if c_evt == BEFORE_TRADING_START:
                    active.append((c_evt, exchange))
        if active:
            controllable.state = controllable.active
            return (ts, BEFORE_TRADING_START, active)
        else:
            return

class OutSession(State):
    def _aggregate(self, controllable, ts, evt, signals):
        if evt == SESSION_START:
            exchanges = controllable.exchanges
            active = []
            flag = False
            for t, c_evt, exchange in signals:
                # filter exchanges
                # todo: map exchange to itself so that we can use get() => faster
                if exchange in exchanges:
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
    def _aggregate(self, controllable, ts, evt, signals):
        return