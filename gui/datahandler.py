from abc import ABC, abstractmethod


class DataHandler(ABC):
    @property
    def name(self):
        return self.__name__

    @abstractmethod
    def start_of_session(self, dt, calendar_name, ledger):
        pass

    @abstractmethod
    def end_of_bar(self, dt, calendar_name, ledger):
        pass

    @abstractmethod
    def end_of_session(self, dt, calendar_name, ledger):
        pass