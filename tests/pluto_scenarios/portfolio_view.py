"""The strategy's own view of the account (context.portfolio, its positions)
must be Zipline's: the limit prices of these orders are computed from it, and
the orders column compares them exactly. Covers cash, portfolio_value,
positions_value, cost_basis (with commissions) and last_sale_price on the
strategy side, after several fills per bar, with a capital base that is not
exactly representable in 32 bits."""
from pluto.api import *

from tests.pluto_scenarios import common


def initialize(context):
    common.set_models(common.default_commission, common.default_slippage)
    context.assets = [symbol(s) for s in ('AAPL', 'MSFT', 'INTC', 'CERN')]
    context.i = 0


def handle_data(context, data):
    context.i += 1
    i = context.i
    portfolio = context.portfolio
    if i % 3 == 1:
        for n, asset in enumerate(context.assets):
            order(asset, 7 * (n + 1) + i)
    elif i % 3 == 2:
        # marketable buy limits: limit = 1 + a function of the account view
        msft = context.assets[1]
        order(msft, 1, limit_price=1000.0 + portfolio.cash / 1000.0)
        order(msft, 1, limit_price=1000.0 + portfolio.portfolio_value / 1000.0)
        order(msft, 1, limit_price=1000.0 + portfolio.positions_value / 1000.0)
    else:
        # sell limits below the price (marketable) from the positions' view
        for asset, position in sorted(portfolio.positions.items()):
            order(asset, -1, limit_price=position.cost_basis * 0.5)
            order(asset, -1, limit_price=position.last_sale_price * 0.5)


def _test_args():
    return common.test_args(
        common.default_commission, common.default_slippage,
        '2014-01-02', '2014-02-28', capital_base=123456.78)
