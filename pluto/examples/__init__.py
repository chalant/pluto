from importlib import import_module

import os

# Columns that we expect to be able to reliably deterministic
# Doesn't include fields that have UUIDS.
_cols_to_check = [
    'algo_volatility',
    'algorithm_period_return',
    'alpha',
    'benchmark_period_return',
    'benchmark_volatility',
    'beta',
    'capital_used',
    'ending_cash',
    'ending_exposure',
    'ending_value',
    'excess_return',
    'gross_leverage',
    'long_exposure',
    'long_value',
    'longs_count',
    'max_drawdown',
    'max_leverage',
    'net_leverage',
    'period_close',
    'period_label',
    'period_open',
    'pnl',
    'portfolio_value',
    'positions',
    'returns',
    'short_exposure',
    'short_value',
    'shorts_count',
    'sortino',
    'starting_cash',
    'starting_exposure',
    'starting_value',
    'trading_days',
    'treasury_period_return',
]

EXAMPLE_MODULES = {}
# These are used by test_examples.py to discover the examples to run.
for f in os.listdir(os.path.dirname(__file__)):
    if not f.endswith('.py') or f == '__init__.py':
        continue
    modname = f[:-len('.py')]
    mod = import_module('pluto.examples.{}'.format(modname))
    EXAMPLE_MODULES[modname] = (mod._test_args(), mod.__spec__.origin)

    # Remove noise from loop variables.
    del f, modname, mod