"""Leverage: a long-short book at gross leverage ~1.6, then every position
closed over the last days of the run (orders to 0, a close via order with the
held amount)."""
from pluto.api import *

from tests.pluto_scenarios import common


def initialize(context):
    common.set_models(common.default_commission, common.default_slippage)
    context.longs = [symbol('AAPL'), symbol('MMM')]
    context.shorts = [symbol('INTC'), symbol('GPS')]
    context.i = 0


def handle_data(context, data):
    context.i += 1
    i = context.i
    if i < 30:
        for asset in context.longs:
            order_target_percent(asset, 0.5)
        for asset in context.shorts:
            order_target_percent(asset, -0.3)
    elif i == 35:
        for asset in context.longs:
            order_target(asset, 0)
    elif i == 37:
        for asset, position in context.portfolio.positions.items():
            order(asset, -position.amount)


def _test_args():
    return common.test_args(
        common.default_commission, common.default_slippage,
        '2013-01-02', '2013-03-28', max_leverage=2.0)
