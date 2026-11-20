# Pluto: the in-memory simulation path

This branch (`restore-sim`, from `origin/dev` `816ecbc2`) restores one path of Pluto to a
working, tested state: **daily backtests of a strategy through Pluto's own
controller/controllable runtime, all in one process** (no gRPC, no database server, no
network). Its results match Zipline's exactly. `RESTORE_PROGRESS.md` at the repository root
has the evidence, the decisions and the full change list of the restoration;
`ALIGN_PROGRESS.md` those of the exact alignment (branch `align-exact`).

The components the owner declared superseded (2026-11-15 to 2026-11-20) were removed
afterwards on branch `cleanup-superseded`: the desktop backtesting window and its launch
command, the market-data ingestion package and the database-backed bar storage it wrote
to, with their dependencies. `CLEANUP_PROGRESS.md` lists every removed file and the items
left for the owner to decide.

## What runs

```
SimulationLoop (24/7 clock)                   pluto/control/loop/simulation_loop.py
  └─ Controller.clock_update                  pluto/control/controller/controller.py
       ├─ SimulationControllerBehavior        pluto/control/controller/simulation_behavior.py
       │     last sale prices from the data portal; data hooks are no-ops
       ├─ StrategyManager                     pluto/control/controller/process_manager.py
       │     account side: ledger, blotter, positions per calendar
       ├─ MarketSimulation                    pluto/finance/markets/market_simulation.py
       │     fills (slippage, commission) against the bundle
       └─ Process -> InMemoryControllableStub pluto/control/processes/, pluto/control/controllable/
             one Controllable per strategy: pluto TradingAlgorithm, clock, pipeline
```

`tests/test_pluto_examples.py` (helpers in `pluto/test_utils/test_utils.py`) wires it up for
the five strategies in `pluto/examples/` on the `test` bundle (`pluto/data/bundles/test.py`,
bcolz data from `tests/resources/example_data.tar.gz`).

## Environment

Python 3.6 with pandas 0.22 (the stored expected results exist only for pandas 0.18.1,
0.19.2 and 0.22.0). Create it, with the exact pins, and build the Cython extensions:

```bash
dev/setup_env.sh ~/.cache/pluto-dev/env36        # conda-forge via a static micromamba, then pip
```

Notable pins: `empyrical==0.5.2` (the stored `alpha` uses its linear annualisation),
`alembic==1.7.7` and `mako==1.1.6` (last versions for Python 3.6), `iso3166==1.0.1`
(newer versions renamed Turkey and break `zipline/country.py`). Rebuild the extensions after
changing a `.pyx`: `python setup.py build_ext --inplace`.

The `protos/*_pb2.py` modules are generated. After changing a `.proto`, regenerate the
module with the environment's `grpcio-tools` (1.33.2), from the repository root:
`$ENV/bin/python -m grpc_tools.protoc -I. --python_out=. protos/<name>.proto`.
Numeric values that the strategy and the controller exchange (orders, transactions, last
sale prices, commissions, run capital, asset tick size) are `double`: a 32-bit `float`
changed limit and stop prices, the strategy's view of its portfolio and the tick rounding.

No configuration file is needed. `~/.pluto_config.toml` (or the file named by
`PLUTO_CONFIG`) is read if present; only the live data source (Alpaca, Polygon) and the
gRPC controllable server use it.

## Tests

```bash
ENV=~/.cache/pluto-dev/env36
$ENV/bin/python -m pytest -p no:cacheprovider \
    tests/test_pluto_examples.py \
    tests/test_pluto_zipline_oracle.py \
    tests/test_pluto_isolation.py \
    tests/test_pluto_assets.py \
    tests/test_pluto_scenarios.py
```

| File | Checks |
|---|---|
| `tests/test_pluto_examples.py` | each example through Pluto, compared with the stored Zipline results on every column both have (`TestMetricsTracker.validate`) |
| `tests/test_pluto_zipline_oracle.py` | each example through the fork's plain Zipline (`zipline.examples.run_example`) and through Pluto in the same process; every common column, exactly |
| `tests/test_pluto_scenarios.py` | the strategies in `tests/pluto_scenarios/` (limit, stop and stop-limit orders, targets, shorts, cancellation, partial fills, commission models, schedule rules and calendars, history, pipeline screen, leverage, splits, dividends, delisting, the strategy's portfolio view) through plain Zipline (`run_algorithm`) and Pluto; every common column, exactly, and no column on one side only |
| `tests/test_pluto_isolation.py` | the examples run in a child interpreter in which the third-party packages that only removed code needed are blocked (in case an older environment still has them) |
| `tests/test_pluto_assets.py` | the asset finder |

Tolerance policy: no comparison is looser than Zipline's own default (`rtol=1e-7, atol=0`).
Pluto against Zipline is exact (`rtol=0, atol=0`), examples and scenarios. Against the
stored results, exact except `momentum_pipeline` (`rtol=2e-12, atol=0`): the fork's own
Zipline differs from the stored file by floating-point summation noise there (largest
relative difference 1.06e-12). All columns are compared, `positions`, `orders` and
`transactions` included (`pluto.test_utils.test_utils.compare_column`: lists of dicts
compared after sorting, identifiers left out, same keys; the stored transactions'
`'commission': None` is dropped, a field of the zipline version that wrote the file).
Zipline reports NaN where Pluto's tracker reports 0 (first-day volatility and ratios); NaN in
the reference counts as 0.

Known differences, not reconciled: recorded variables are not produced on the Pluto side
(the example ports do not call `record()`; the controller-side tracker has no channel for
them). Scenario strategies must use the generic `EquityPricing` columns in pipelines: Pluto's
pipeline domain has no loader for `USEquityPricing`.

The rest of `tests/` is Zipline's own suite. Most of it does not run on the fork (fixtures
break on the fork's changes); it is out of scope.

## Out of scope (not repaired, still in the tree)

- the gRPC services (`pluto/control/controllable/server.py`, `pluto/interface/`, `dev/cli.py`),
- live and paper trading, the old controller modes (`pluto/control/modes/`),
- the launcher support that only the removed window used (`pluto/control/script_handler.py`,
  `pluto/control/setup/`, `python __main__.py add-manager`) and `dev/backtester.py`.

`python __main__.py install-examples` copies the examples into the strategy store under
`~/.pluto` and ingests the test bundle.
