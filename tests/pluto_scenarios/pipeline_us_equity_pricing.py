"""A pipeline built on the US-specific columns (USEquityPricing: open, high,
low, close, volume) with built-in factors, a CustomFactor and a screen: the
top 5 by 10-day average dollar volume with a 5-day average close above 20;
weights from the rank of the 10-day returns and from the CustomFactor."""
import numpy as np

from pluto.pipeline import Pipeline
from pluto.pipeline.factors import (
    AverageDollarVolume, Returns, SimpleMovingAverage)
from zipline.pipeline import CustomFactor
from zipline.pipeline.data import USEquityPricing

from pluto.api import *

from tests.pluto_scenarios import common


class CloseLocation(CustomFactor):
    """Mean over the window of (close - open) / (high - low), 0 where the
    range is empty."""
    inputs = [USEquityPricing.open, USEquityPricing.high,
              USEquityPricing.low, USEquityPricing.close]
    window_length = 5

    def compute(self, today, assets, out, open_, high, low, close):
        span = high - low
        loc = np.where(span > 0, (close - open_) / np.where(span > 0, span, 1.0), 0.0)
        out[:] = np.nanmean(loc, axis=0)


def make_pipeline():
    dollar_volume = AverageDollarVolume(
        inputs=[USEquityPricing.close, USEquityPricing.volume],
        window_length=10)
    sma = SimpleMovingAverage(inputs=[USEquityPricing.close], window_length=5)
    returns = Returns(inputs=[USEquityPricing.close], window_length=10)
    location = CloseLocation()
    screen = dollar_volume.top(5) & (sma > 20.0)
    return Pipeline(
        columns={
            'dv': dollar_volume,
            'sma': sma,
            'returns': returns,
            'rank': returns.rank(mask=screen),
            'location': location,
        },
        screen=screen,
    )


def initialize(context):
    common.set_models(common.default_commission, common.default_slippage)
    attach_pipeline(make_pipeline(), 'us')
    schedule_function(rebalance, date_rules.every_day())


def before_trading_start(context, data):
    context.output = pipeline_output('us')


def rebalance(context, data):
    output = context.output
    weights = {}
    for asset, row in output.iterrows():
        # rank 1..5 plus the close location (in [-1, 1]); at least 0.5
        # (also when the rank is NaN)
        weight = row['rank'] + row['location']
        weights[asset] = weight if weight > 0.5 else 0.5
    total = sum(weights.values())
    for asset in sorted(weights):
        if data.can_trade(asset):
            order_target_percent(asset, 0.9 * weights[asset] / total)
    for asset in list(context.portfolio.positions):
        if asset not in weights and data.can_trade(asset):
            order_target_percent(asset, 0)


def _test_args():
    return common.test_args(
        common.default_commission, common.default_slippage,
        '2013-10-07', '2013-12-31', look_back=150)
