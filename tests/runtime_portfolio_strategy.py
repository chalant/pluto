"""The strategy of tests/test_pluto_runtime_portfolio.py: it records what it sees of its own portfolio and account at some
handle_data (the runtime's view: ``context.portfolio`` and ``context.account``), with fills, price changes and
commissions every day. The records are kept in RECORDS of this module, which both the plain Zipline run (it imports
this module) and Pluto (its script imports it) append to."""
from pluto.api import *

from tests.pluto_scenarios import common

RECORDS = []


def initialize(context):
    common.set_models(common.default_commission, common.default_slippage)
    context.assets = [symbol(s) for s in ('AAPL', 'MSFT', 'INTC', 'CERN')]
    context.i = 0


def handle_data(context, data):
    from tests import runtime_portfolio_strategy as records

    context.i += 1
    i = context.i

    # what the strategy sees: compounded returns and pnl depend on where the runtime's ledger was revalued. It reads
    # its portfolio only every fourth day: in between, the revaluations are the ones the runtime makes by itself
    # (at the end of each session), one per day, each compounding returns and pnl
    if i % 4 == 0:
        portfolio = context.portfolio
        account = context.account

        records.RECORDS.append((
            portfolio.returns, portfolio.pnl, portfolio.portfolio_value, portfolio.cash, portfolio.positions_value,
            account.leverage, account.net_leverage))

    if i % 2 == 1:
        for n, asset in enumerate(context.assets):
            order(asset, 5 * (n + 1) + i)
    else:
        for asset in context.assets:
            order(asset, -3)


def _test_args():
    return common.test_args(
        common.default_commission, common.default_slippage,
        '2014-01-02', '2014-03-31', capital_base=123456.78)
