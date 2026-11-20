"""Helpers shared by the scenario strategies (tests/test_pluto_scenarios.py).

Each scenario module is a strategy that runs unchanged in the fork's plain
Zipline (run_algorithm) and in Pluto (the strategy file is executed by the
controllable). The api functions are the same objects in pluto.api and
zipline.api (zipline/utils/api_support.py), so `from pluto.api import *` works
in both.

Commission and slippage: Zipline takes them from set_commission/set_slippage in
initialize; in Pluto those calls are no-ops and the controller side takes the
models from _test_args (commission_models, slippage_models). Each scenario
defines the factories once and uses both paths; fresh instances per run (the
slippage models keep per-bar state).
"""
import pandas as pd

from zipline.finance import commission, slippage


def test_args(commission_factory, slippage_factory, start, end,
              capital_base=1e5, max_leverage=1.0, look_back=None):
    return {
        'start': pd.Timestamp(start, tz='utc'),
        'end': pd.Timestamp(end, tz='utc'),
        'capital_base': capital_base,
        'max_leverage': max_leverage,
        'look_back': look_back,
        'data_frequency': 'daily',
        'commission_models': {
            'equity': commission_factory(),
            'futures': commission.NoCommission()
        },
        'slippage_models': {
            'equity': slippage_factory(),
            'futures': slippage.NoSlippage()
        }
    }


def set_models(commission_factory, slippage_factory):
    """Zipline: sets the models; Pluto: no-ops (models from _test_args)."""
    from pluto import api
    api.set_commission(us_equities=commission_factory())
    api.set_slippage(us_equities=slippage_factory())


def default_commission():
    return commission.PerShare(cost=.0075, min_trade_cost=1.0)


def default_slippage():
    return slippage.VolumeShareSlippage()
