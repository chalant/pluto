import math
import abc


class Broker(abc.ABC):
    @abc.abstractmethod
    def compute_capital(self, ratio):
        pass

    @abc.abstractmethod
    def adjust_max_leverage(self, max_leverage):
        pass

    @abc.abstractmethod
    def update(self, dt, evt):
        return


class SimulationBroker(Broker):
    def __init__(self, capital, max_leverage):
        self._capital = capital
        self._max_leverage = max_leverage

    def compute_capital(self, ratio):
        return math.floor(self._capital * ratio)

    def adjust_max_leverage(self, max_leverage):
        return max_leverage


class LiveBroker(Broker):
    def __init__(self):
        pass

    def compute_capital(self, ratio):
        pass

    def adjust_max_leverage(self, max_leverage):
        pass
