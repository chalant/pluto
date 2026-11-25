from abc import ABC, abstractmethod

import pandas


class AbstractMarket(ABC):
    @abstractmethod
    def get_transactions(self, calendar_name: str, blotter):
        pass

    @abstractmethod
    def set_calendar_timestamp(self, calendar_name: str, ts: pandas.Timestamp) -> None:
        pass
