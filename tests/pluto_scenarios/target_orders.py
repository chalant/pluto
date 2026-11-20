"""order_target, order_target_value, order_target_percent, order_value and
order_percent on several assets, rebalanced every day."""
from pluto.api import *

from tests.pluto_scenarios import common


def initialize(context):
    common.set_models(common.default_commission, common.default_slippage)
    context.assets = [symbol(s) for s in ('AAPL', 'MSFT', 'INTC', 'GPS', 'COST', 'MMM')]
    context.i = 0


def handle_data(context, data):
    context.i += 1
    i = context.i
    aapl, msft, intc, gps, cost, mmm = context.assets
    order_target(aapl, 10 + (i % 5) * 7)
    order_target_value(msft, 5000 + (i % 3) * 1500)
    order_target_percent(intc, 0.05 + (i % 4) * 0.02)
    if i % 6 == 1:
        order_value(gps, 3000)
    if i % 6 == 4:
        order_percent(gps, -0.02)
    order_target_percent(cost, 0.1 if i % 2 else 0.12)
    if i % 10 == 0:
        order_target(mmm, 0)
    else:
        order_target_value(mmm, 4000)


def _test_args():
    return common.test_args(
        common.default_commission, common.default_slippage,
        '2013-03-01', '2013-06-28')
