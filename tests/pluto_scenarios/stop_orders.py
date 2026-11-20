"""Stop orders: a protective sell stop below the price on a long position
(triggers later), a buy stop above the price, a stop that never triggers."""
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
        order(context.aapl, 100)
        price = data.current(context.msft, 'price')
        order(context.msft, 100, stop_price=price * 1.02)
    elif i == 2:
        price = data.current(context.aapl, 'price')
        order(context.aapl, -100, stop_price=price * 0.97)
        order(context.aapl, -10, style=execution.StopOrder(price * 0.5))


def _test_args():
    return common.test_args(
        common.default_commission, common.default_slippage,
        '2014-01-02', '2014-03-31')
