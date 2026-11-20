"""schedule_function with week_start and month_start date rules (no
handle_data)."""
from pluto.api import *

from tests.pluto_scenarios import common


def initialize(context):
    common.set_models(common.default_commission, common.default_slippage)
    context.aapl = symbol('AAPL')
    context.msft = symbol('MSFT')
    context.weeks = 0
    schedule_function(weekly, date_rules.week_start(days_offset=1))
    schedule_function(monthly, date_rules.month_start())


def weekly(context, data):
    context.weeks += 1
    order(context.aapl, 5 if context.weeks % 2 else -3)


def monthly(context, data):
    order_target_percent(context.msft, 0.3)


def _test_args():
    return common.test_args(
        common.default_commission, common.default_slippage,
        '2013-01-02', '2013-06-28')
