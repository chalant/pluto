"""PerDollar commission with FixedBasisPointsSlippage and the
VolumeShareSlippage default volume limit."""
from zipline.finance import commission, slippage

from pluto.api import *

from tests.pluto_scenarios import common


def _commission():
    return commission.PerDollar(cost=0.0015)


def _slippage():
    return slippage.FixedBasisPointsSlippage(basis_points=5, volume_limit=0.1)


def initialize(context):
    common.set_models(_commission, _slippage)
    context.aapl = symbol('AAPL')
    context.msft = symbol('MSFT')
    context.i = 0


def handle_data(context, data):
    context.i += 1
    if context.i % 4 == 1:
        order_value(context.aapl, 10000)
        order_target_value(context.msft, -8000 if context.i % 8 == 1 else 3000)


def _test_args():
    return common.test_args(
        _commission, _slippage, '2014-01-02', '2014-03-31')
