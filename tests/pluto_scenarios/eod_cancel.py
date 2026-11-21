"""EODCancel cancel policy (set_cancel_policy) in a daily run: zipline-reloaded
(698b7dbb, "enabled EODCancel for daily frequency") executes the daily cancel
policy at the end of each session: of the open orders of an asset that has more
than one, the oldest is canceled (the unfilled limit order of the first bar when
the second one is placed); the partially filled market orders (small volume
limit) keep filling on the next bars. Pluto must do the same."""
from zipline.finance import slippage

from pluto.api import *

from tests.pluto_scenarios import common


def _slippage():
    return slippage.VolumeShareSlippage(volume_limit=0.0005)


def initialize(context):
    common.set_models(common.default_commission, _slippage)
    set_cancel_policy(cancel_policy.EODCancel())
    context.aapl = symbol('AAPL')
    context.msft = symbol('MSFT')
    context.i = 0


def handle_data(context, data):
    context.i += 1
    if context.i in (1, 4):
        price = data.current(context.aapl, 'price')
        order(context.aapl, 100, limit_price=price * 0.9)
        order(context.msft, 30000)


def _test_args():
    return common.test_args(
        common.default_commission, _slippage,
        '2014-01-02', '2014-01-31', capital_base=1e7)
