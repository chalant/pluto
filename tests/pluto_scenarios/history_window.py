"""data.history: moving averages over windows that reach before the start of
the run, used to decide orders."""
from pluto.api import *

from tests.pluto_scenarios import common


def initialize(context):
    common.set_models(common.default_commission, common.default_slippage)
    context.assets = [symbol('AAPL'), symbol('MSFT'), symbol('INTC')]


def handle_data(context, data):
    prices = data.history(context.assets, 'price', 20, '1d')
    volumes = data.history(context.assets, 'volume', 5, '1d')
    for asset in context.assets:
        short = prices[asset][-5:].mean()
        long_ = prices[asset].mean()
        if short > long_ and volumes[asset].mean() > 0:
            order_target_percent(asset, 0.2)
        else:
            order_target(asset, 0)


def _test_args():
    return common.test_args(
        common.default_commission, common.default_slippage,
        '2013-03-01', '2013-05-31')
