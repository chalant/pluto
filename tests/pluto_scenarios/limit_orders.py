"""Limit orders: a buy limit far below the price (never fills), a buy limit a
little below the price (fills on a later bar), a sell limit above the price on
the position (fills later), a short-side sell limit."""
from pluto.api import *

from tests.pluto_scenarios import common


def initialize(context):
    common.set_models(common.default_commission, common.default_slippage)
    context.aapl = symbol('AAPL')
    context.msft = symbol('MSFT')
    context.i = 0


def handle_data(context, data):
    context.i += 1
    i = context.i
    if i == 1:
        price = data.current(context.aapl, 'price')
        order(context.aapl, 100, limit_price=price * 0.5)
        order(context.aapl, 50, limit_price=price * 0.98)
        order(context.msft, 200)
    elif i == 3:
        price = data.current(context.msft, 'price')
        order(context.msft, -200, limit_price=price * 1.03)
    elif i == 5:
        price = data.current(context.msft, 'price')
        order(context.msft, -100, style=execution.LimitOrder(price * 1.01))


def _test_args():
    return common.test_args(
        common.default_commission, common.default_slippage,
        '2014-01-02', '2014-03-31')
