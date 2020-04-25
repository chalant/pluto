import math
import abc

class Broker(abc.ABC):
    @abc.abstractmethod
    def compute_capital(self, ratio):
        raise NotImplementedError

    @abc.abstractmethod
    def adjust_max_leverage(self, max_leverage):
        raise NotImplementedError

    def update(self, dt, evt, signals):
        return self._update(dt, evt, signals)

    @abc.abstractmethod
    def _update(self, dt, evt, signals):
        raise NotImplementedError

    def add_market(self, session_id, start, end, universe_name):
        pass


class SimulationBroker(Broker):
    def __init__(self, capital, max_leverage):
        self._capital = capital
        self._max_leverage = max_leverage

    def compute_capital(self, ratio):
        return math.floor(self._capital * ratio)

    def adjust_max_leverage(self, max_leverage):
        return max_leverage

    def _update(self, dt, evt, signals):
        return


class LiveBroker(Broker):
    def __init__(self):
        pass

    def compute_capital(self, ratio):
        pass

    def adjust_max_leverage(self, max_leverage):
        pass

class LiveSimulationBroker(Broker):
    def __init__(self, capital, max_leverage, market_factory):
        '''

        Parameters
        ----------
        capital: float
        max_leverage: float
        market_factory: pluto.control.modes.market.factory.MarketFactory
        '''
        self._capital = capital
        self._max_leverage = max_leverage
        self._market_factory = market_factory

    def _update(self, dt, evt, signals):
        return self._market_factory.get_transactions(dt, evt, signals)

    def compute_capital(self, ratio):
        return math.floor(self._capital * ratio)

    def adjust_max_leverage(self, max_leverage):
        pass

    def add_market(self, session_id, start, end, universe_name):
        mkt = self._market_factory.get_market(start, end, universe_name)
        mkt.add_blotter(session_id)
