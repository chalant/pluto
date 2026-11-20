"""Delisting: DELL stops trading on 2013-10-29 (auto close 2013-10-30) while
the strategy holds a long position and has an open limit order on it; the
position is closed and the order cancelled by the expired-assets cleanup."""
from pluto.api import *

from tests.pluto_scenarios import common


def initialize(context):
    common.set_models(common.default_commission, common.default_slippage)
    context.dell = symbol('DELL')
    context.msft = symbol('MSFT')
    context.i = 0


def handle_data(context, data):
    context.i += 1
    if context.i == 1:
        order(context.dell, 500)
        order(context.msft, 100)
    elif context.i == 5:
        price = data.current(context.dell, 'price')
        order(context.dell, 300, limit_price=price * 0.5)


def _test_args():
    return common.test_args(
        common.default_commission, common.default_slippage,
        '2013-10-01', '2013-11-15')
