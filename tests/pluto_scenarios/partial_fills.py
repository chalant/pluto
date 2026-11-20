"""Partial fills: VolumeShareSlippage with a small volume limit, orders much
larger than the volume share, so that they fill over several bars."""
from zipline.finance import slippage

from pluto.api import *

from tests.pluto_scenarios import common


def _slippage():
    return slippage.VolumeShareSlippage(volume_limit=0.0005, price_impact=0.1)


def initialize(context):
    common.set_models(common.default_commission, _slippage)
    context.aapl = symbol('AAPL')
    context.cern = symbol('CERN')
    context.i = 0


def handle_data(context, data):
    context.i += 1
    i = context.i
    if i == 1:
        order(context.aapl, 20000)
        order(context.cern, 30000)
    elif i == 12:
        order(context.cern, -50000)


def _test_args():
    return common.test_args(
        common.default_commission, _slippage,
        '2013-01-02', '2013-03-28', capital_base=1e7)
