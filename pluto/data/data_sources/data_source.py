import abc
import typing
import collections

import pandas

from zipline.data import data_portal
from zipline.assets import Asset
from zipline.protocol import Position


class AbstractDataSource(abc.ABC):
    @abc.abstractmethod
    def get_last_sale_prices(self,
                             targets: typing.List[
                                typing.Tuple[
                                    pandas.Timestamp,
                                    pandas.Timestamp,
                                    str,
                                    typing.List[Position]]],
                             handle_non_market_minutes=False) -> typing.Dict[
            str,
            typing.List[typing.Tuple[int, float]]]:
        pass

    @abc.abstractmethod
    def get_bar_data(self, ts: pandas.Timestamp, assets):
        pass


class SimulationDataSource(AbstractDataSource):
    def __init__(self, data_frequency):
        self._data_portals = {}
        self._data_frequency = data_frequency

    def get_last_sale_prices(self, targets: typing.List[
        typing.Tuple[
            pandas.Timestamp,
            pandas.Timestamp,
            str,
            typing.List[Position]]],
            handle_non_market_minutes=False) -> typing.Dict[str, typing.List[typing.Tuple[int, float]]]:

        dp = self._data_portals
        res = collections.defaultdict(list)
        data_frequency = self._data_frequency

        for ts, cts, cal, positions in targets:
            portal = dp[cal]

            for position in positions:
                if position.amount != 0:
                    asset = position.asset
                    if not handle_non_market_minutes:
                        price = portal.get_scalar_asset_spot_value(
                            asset,
                            field='price',
                            dt=cts,
                            data_frequency=data_frequency)
                    else:
                        price = portal.get_adjusted_value(
                            asset,
                            field='price',
                            dt=cts,
                            data_frequency=data_frequency)
                    res[cal].append((asset.sid, price))
        return res

    def get_bar_data(self, ts: pandas.Timestamp, assets):
        return

    def add_data_portal(self, calendar_name: str, dp: data_portal.DataPortal):
        self._data_portals[calendar_name] = dp
