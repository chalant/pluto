from collections import deque

from pandas import DatetimeIndex

from pluto.control.controllable import controllable
from pluto.finance.blotter import liveblotter
from pluto import algorithm

class LiveControllable(controllable.Controllable):
    def __init__(self, broker_stub):
        super(LiveControllable, self).__init__()
        self._broker = broker_stub
        self._sessions_array = deque()
        self._last_session_update = None

    def _get_algorithm_class(self):
        return algorithm.LiveTradingAlgorithm

    def _get_sessions(self, dt, params):
        sessions = self._sessions_array
        if sessions[-1] != dt:
            sessions.popleft()
            sessions.append(dt)
            params.start_session = sessions[0].normalize()
            params.end_session = dt
            return DatetimeIndex(sessions)
        else:
            return self._sessions

    def _update_blotter(self, blotter, broker_data):
        blotter.update(broker_data)

    def _create_blotter(self):
        return liveblotter.LiveBlotter(self._broker)

    def _update_account(self, blotter, main_account):
        blotter.update_account(main_account)
