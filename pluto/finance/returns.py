import collections

import numpy as np


class ReturnsTracker(object):
    def __init__(self, ledger, look_back):
        self._session_count = -1

        self.daily_returns_array = collections.deque()
        self._previous_total_returns = 0

        self._ledger = ledger
        self._look_back = look_back
        self._sessions = []

    @property
    def daily_returns(self):
        return np.array(self.daily_returns_array)

    def todays_returns(self, returns):
        return (returns + 1) / (self._previous_total_returns + 1) - 1

    def start_of_session(self, session_label):
        self._session_count += 1
        self._previous_total_returns = self._ledger.returns

    # todo: check this! we append returns
    def end_of_bar(self, data_frequency):
        if data_frequency == 'minute':
            self.daily_returns_array.append(self.todays_returns(self._ledger.returns))

    def end_of_session(self):
        returns = self.daily_returns_array
        portfolio_returns = self._ledger.returns
        # if self._data_frequency == 'daily':

        # todo: only pop when in live ?
        if self._session_count == self._look_back:
            returns.popleft()
            self._session_count = 0

        returns.append(self.todays_returns(portfolio_returns))
