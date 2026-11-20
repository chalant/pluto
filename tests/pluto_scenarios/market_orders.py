"""Market orders with order(): buy, partial sell, sell through zero into a
short, cover, close."""
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
        order(context.msft, 200)
    elif i == 5:
        order(context.aapl, -50)
    elif i == 10:
        order(context.msft, -300)
    elif i == 15:
        order(context.msft, 100)
    elif i == 20:
        order(context.aapl, -50)


def _test_args():
    return common.test_args(
        common.default_commission, common.default_slippage,
        '2014-01-02', '2014-03-31')
