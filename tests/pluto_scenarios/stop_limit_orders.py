"""Stop-limit orders: a buy stop-limit whose stop triggers on a later bar,
then fills within the limit; a protective sell stop-limit that triggers on a
later bar and fills above its limit; a buy stop-limit whose stop is never
reached (stays open)."""
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
        price = data.current(context.msft, 'price')
        order(context.msft, 100, limit_price=price * 1.05, stop_price=price * 1.01)
        order(context.aapl, 100)
        order(context.msft, 10, style=execution.StopLimitOrder(price * 1.6, price * 1.5))
    elif i == 2:
        price = data.current(context.aapl, 'price')
        order(context.aapl, -100, style=execution.StopLimitOrder(price * 0.80, price * 0.97))


def _test_args():
    return common.test_args(
        common.default_commission, common.default_slippage,
        '2014-01-02', '2014-03-31')
