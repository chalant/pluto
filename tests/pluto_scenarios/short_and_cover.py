"""Short positions: open shorts with order and order_target_percent, add to a
short, partially cover, cover fully, flip from short to long."""
from pluto.api import *

from tests.pluto_scenarios import common


def initialize(context):
    common.set_models(common.default_commission, common.default_slippage)
    context.msft = symbol('MSFT')
    context.intc = symbol('INTC')
    context.i = 0


def handle_data(context, data):
    context.i += 1
    i = context.i
    if i == 1:
        order(context.msft, -300)
        order_target_percent(context.intc, -0.2)
    elif i == 6:
        order(context.msft, -100)
    elif i == 12:
        order(context.msft, 150)
    elif i == 20:
        order_target(context.msft, 0)
        order_target_percent(context.intc, 0.1)
    elif i == 30:
        order_target(context.intc, 0)


def _test_args():
    return common.test_args(
        common.default_commission, common.default_slippage,
        '2013-01-02', '2013-03-28')
