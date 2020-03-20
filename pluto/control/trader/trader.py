from abc import ABC, abstractmethod

class Trader(ABC):

    @abstractmethod
    def start(self, broker_url):
        raise NotImplementedError

    @abstractmethod
    def stop(self):
        raise NotImplementedError

    def clock_update(self, clock_evt):
        pass
