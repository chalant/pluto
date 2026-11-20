"""A pipeline with a filter and a screen: top 4 by 10-day average dollar
volume, screened to prices above 20; equal-weight the screened assets."""
from pluto.pipeline import Pipeline
from pluto.pipeline.factors import AverageDollarVolume, SimpleMovingAverage
from zipline.pipeline.data import EquityPricing

from pluto.api import *

from tests.pluto_scenarios import common


def make_pipeline():
    dollar_volume = AverageDollarVolume(window_length=10)
    top = dollar_volume.top(4)
    sma = SimpleMovingAverage(inputs=[EquityPricing.close], window_length=5)
    return Pipeline(
        columns={'sma': sma, 'dv': dollar_volume},
        screen=top & (sma > 20.0),
    )


def initialize(context):
    common.set_models(common.default_commission, common.default_slippage)
    attach_pipeline(make_pipeline(), 'screened')
    schedule_function(rebalance, date_rules.every_day())


def before_trading_start(context, data):
    context.output = pipeline_output('screened')


def rebalance(context, data):
    assets = list(context.output.index)
    for asset in assets:
        if data.can_trade(asset):
            order_target_percent(asset, 1.0 / max(len(assets), 1))
    for asset in list(context.portfolio.positions):
        if asset not in assets and data.can_trade(asset):
            order_target_percent(asset, 0)


def _test_args():
    return common.test_args(
        common.default_commission, common.default_slippage,
        '2013-10-07', '2013-12-31', look_back=150)
