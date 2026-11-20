"""Splits: positions held across CERN's 2:1 split (2013-07-01) and AAPL's 7:1
split (2014-06-09), and an open limit order on AAPL across its split."""
from pluto.api import *

from tests.pluto_scenarios import common


def initialize(context):
    common.set_models(common.default_commission, common.default_slippage)
    context.aapl = symbol('AAPL')
    context.cern = symbol('CERN')
    context.i = 0


def handle_data(context, data):
    context.i += 1
    dt = get_datetime().date()
    if context.i == 1:
        order(context.cern, 101)
        order(context.aapl, 33)
    if str(dt) == '2014-06-05':
        price = data.current(context.aapl, 'price')
        order(context.aapl, 7, limit_price=price * 0.5)
        order(context.aapl, 3)


def _test_args():
    return common.test_args(
        common.default_commission, common.default_slippage,
        '2013-06-03', '2014-06-30')
