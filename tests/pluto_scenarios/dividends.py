"""Cash dividends: positions in INTC, MSFT and COST held across several
ex-dates (the test bundle has cash dividends for them; their pay dates are
missing, NaT)."""
from pluto.api import *

from tests.pluto_scenarios import common


def initialize(context):
    common.set_models(common.default_commission, common.default_slippage)
    context.assets = [symbol('INTC'), symbol('MSFT'), symbol('COST')]
    context.i = 0


def handle_data(context, data):
    context.i += 1
    if context.i == 1:
        for asset in context.assets:
            order(asset, 100)
    elif context.i == 60:
        order(context.assets[0], -100)


def _test_args():
    return common.test_args(
        common.default_commission, common.default_slippage,
        '2013-01-02', '2013-12-31')
