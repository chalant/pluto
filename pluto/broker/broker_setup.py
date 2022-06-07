import math

import numpy as np

from zipline.utils import math_utils


def _check_order_types_and_asset_types(asset, order_type, trade_setup):
    if type(asset).__name__ not in trade_setup.asset_types:
        return False

    if order_type not in trade_setup.order_types:
        return False

    return True


def _can_order_whole(trade_setup, amount, asset, order_type):
    amount = math_utils.round_if_near_integer(amount)

    amount = (amount < 0.0) * np.ceil(amount) + (amount >= 0.0) * np.floor(amount)

    ts = trade_setup

    if amount < 0.0 and ts.can_short is False:
        return amount, False

    return amount, _check_order_types_and_asset_types(
        asset,
        order_type,
        ts)


class FractionalTradeSetup(object):
    __slots__ = ['minimum_purchase', 'minimal_fraction', 'order_types', 'asset_types', 'can_short']

    def __init__(self):
        self.minimum_purchase = 0.0
        self.minimal_fraction = 3
        self.order_types = set()
        self.asset_types = set()
        self.can_short = False


class TradeSetup(object):
    __slots__ = ['order_types', 'asset_types', 'can_short']

    def __init__(self):
        self.order_types = set()
        self.asset_types = set()
        self.can_short = True


class BrokerSetup(object):
    """
    Class used for checking if an order can be executed depending on what is
    supported by the broker.

    """
    def __init__(self, fts=None, ts=None, fractional=False):
        self.fractional_trade_setup = FractionalTradeSetup() if fts is None else fts
        self.trade_setup = TradeSetup() if ts is None else ts
        self.fractional = fractional

    def can_order(self, asset, order_style, amount, current_data):
        fts = self.fractional_trade_setup
        order_type = type(order_style).__name__

        if not self.fractional:
            return _can_order_whole(
                self.trade_setup,
                amount,
                asset,
                order_type)

        else:
            abs_amount = abs(round(amount, fts.minimal_fraction))
            whole_amount = math.floor(abs_amount)

            fractional_amount = round(abs_amount - whole_amount, fts.minimal_fraction)

            if fractional_amount > 0.0:

                if amount < 0.0 and fts.can_short is False:
                    return amount, False

                value = current_data.current(asset, "price") * abs_amount

                if value < fts.minimum_purchase:
                    return amount, False

                return amount, _check_order_types_and_asset_types(
                    asset,
                    order_type,
                    fts)
            else:
                return _can_order_whole(
                    self.trade_setup,
                    amount,
                    asset,
                    order_type)


def load_fractional_trade_setup(proto):
    self = FractionalTradeSetup()

    self.minimal_fraction = proto.minimal_fraction
    self.minimum_purchase = proto.minimum_purchase
    self.order_types = set(proto.order_types)
    self.asset_types = set(proto.asset_types)
    self.can_short = proto.can_short

    return self


def load_trade_setup(proto):
    self = TradeSetup()

    self.order_types = set(proto.order_types)
    self.asset_types = set(proto.asset_types)
    self.can_short = proto.can_short

    return self


def load_broker_setup(proto):
    return BrokerSetup(
        load_fractional_trade_setup(proto.f_trade_setup),
        load_trade_setup(proto.trade_setup),
        proto.fractional
    )
