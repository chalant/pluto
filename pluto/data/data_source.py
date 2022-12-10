import abc
import typing
import collections

import pandas


from zipline.data import data_portal
from zipline.assets import Asset


class AbstractDataSource(abc.ABC):
    @abc.abstractmethod
    def get_last_sale_prices(self,
                             targets: typing.List[
                                typing.Tuple[
                                    pandas.Timestamp,
                                    pandas.Timestamp,
                                    str,
                                    typing.List[Asset]]],
                             data_frequency,
                             handle_non_market_minutes=False) -> typing.Dict[
            str,
            typing.List[typing.Tuple[int, float]]]:
        pass


class SimulationDataSource(AbstractDataSource):
    def __init__(self):
        self._data_portals = {}

    def get_last_sale_prices(self, targets: typing.List[
        typing.Tuple[
            pandas.Timestamp,
            pandas.Timestamp,
            str,
            typing.List[Asset]]],
            data_frequency,
            handle_non_market_minutes=False) -> typing.Dict[str, typing.List[typing.Tuple[int, float]]]:

        dp = self._data_portals
        res = collections.defaultdict(list)

        for ts, cts, cal, assets in targets:
            portal = dp[cal]

            for asset in assets:
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
                        data_frequency=data_frequency
                    )

                res[cal].append((asset.sid, price))

        return res

    def ingest_bar_data(self, arr: typing.List[typing.Tuple[pandas.Timestamp, pandas.Timestamp, str]]):
        pass

    def update_asset_database(self, arr: typing.List[typing.Tuple[pandas.Timestamp, pandas.Timestamp, str]]):
        pass

    def add_data_portal(self, calendar_name: str, dp: data_portal.DataPortal):
        self._data_portals[calendar_name] = dp
