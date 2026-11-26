import collections
import typing

from zipline.data import data_portal

from protos.clock_pb2 import (
    SESSION_START,
    BEFORE_TRADING_START,
    BAR,
    MINUTE_END,
    TRADE_END,
    SESSION_END)

from pluto.control.controller.behavior import AbstractControllerBehavior


class SimulationControllerBehavior(AbstractControllerBehavior):
    def __init__(self, data_frequency):
        self._data_portals = {}
        self._data_frequency = data_frequency

    def get_last_sale_prices(self, ts, manager, signals, handle_non_market_minutes=False) -> \
            typing.Dict[str, typing.List[typing.Tuple[int, float]]]:

        dp = self._data_portals
        res = collections.defaultdict(list)
        data_frequency = self._data_frequency

        targets = [
            (cts, calendar.name, manager.broker.get_positions(calendar.name))
            for cts, c_evt, calendar in signals
            if c_evt == BAR or
               c_evt == MINUTE_END or
               c_evt == TRADE_END or
               c_evt == SESSION_END or
               c_evt == BEFORE_TRADING_START]

        for cts, cal, positions in targets:
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

    # in simulation all the data is already in the bundle: nothing to ingest.
    def update_data(self, ts, manager, signals):
        pass

    def ingest_all_data(self, ts, manager, signals):
        pass

    def add_data_portal(self, calendar_name: str, dp: data_portal.DataPortal):
        self._data_portals[calendar_name] = dp