import typing
from abc import ABC, abstractmethod

import pandas as pd

from pluto.control.controller import process_manager as pm


class AbstractControllerBehavior(ABC):
    @abstractmethod
    def get_last_sale_prices(self, ts, manager, signals, handle_non_market_minutes=False):
        pass

    @abstractmethod
    def update_watchlist(self, manager: pm.StrategyManager):
        pass

    @abstractmethod
    def update_data(self, ts: pd.Timestamp, manager: pm.StrategyManager, signals: typing.List):
        pass
