"""schedule_function's calendar argument: rules on calendars.US_EQUITIES and
on calendars.US_FUTURES (zipline evaluates the date rule on that calendar's
sessions), over a year with holidays."""
from pluto.api import *

from tests.pluto_scenarios import common


def initialize(context):
    common.set_models(common.default_commission, common.default_slippage)
    context.aapl = symbol('AAPL')
    context.msft = symbol('MSFT')
    schedule_function(buy_aapl, date_rules.week_end(days_offset=1),
                      calendar=calendars.US_EQUITIES)
    schedule_function(buy_msft, date_rules.month_end(days_offset=1),
                      calendar=calendars.US_FUTURES)
    schedule_function(sell_msft, date_rules.week_start(days_offset=0),
                      calendar=calendars.US_FUTURES)


def buy_aapl(context, data):
    order(context.aapl, 2)


def buy_msft(context, data):
    order(context.msft, 40)


def sell_msft(context, data):
    order(context.msft, -3)


def _test_args():
    return common.test_args(
        common.default_commission, common.default_slippage,
        '2013-01-02', '2013-12-31')
