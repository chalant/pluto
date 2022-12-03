from abc import ABC, abstractmethod

from trading_calendars import trading_calendar


class CompoundCalendar(trading_calendar.TradingCalendar, ABC):
    @abstractmethod
    @property
    def base_calendars(self):
        pass