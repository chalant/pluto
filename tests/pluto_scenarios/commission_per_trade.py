"""PerTrade commission with FixedSlippage."""
from zipline.finance import commission, slippage

from pluto.api import *

from tests.pluto_scenarios import common


def _commission():
    return commission.PerTrade(cost=5.0)


def _slippage():
    return slippage.FixedSlippage(spread=0.02)


def initialize(context):
    common.set_models(_commission, _slippage)
    context.aapl = symbol('AAPL')
    context.msft = symbol('MSFT')
    context.i = 0


def handle_data(context, data):
    context.i += 1
    if context.i % 3 == 1:
        order(context.aapl, 10)
        order(context.msft, -25)


def _test_args():
    return common.test_args(
        _commission, _slippage, '2014-01-02', '2014-02-28')
