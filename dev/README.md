# Pluto: the in-memory simulation path

This branch (`restore-sim`, from `origin/dev` `816ecbc2`) restores one path of Pluto to a
working, tested state: **daily backtests of a strategy through Pluto's own
controller/controllable runtime, all in one process** (no gRPC, no database server, no
network). Its results match Zipline's. `RESTORE_PROGRESS.md` at the repository root has the
evidence, the decisions and the full change list.

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
    tests/test_pluto_assets.py
```

| File | Checks |
|---|---|
| `tests/test_pluto_examples.py` | each example through Pluto, compared with the stored Zipline results (rtol 0.001, atol 0.02) |
| `tests/test_pluto_zipline_oracle.py` | each example through the fork's plain Zipline (`zipline.examples.run_example`) and through Pluto in the same process; all common columns, incl. positions, orders, transactions |
| `tests/test_pluto_isolation.py` | the examples run in a child interpreter in which the third-party packages that only removed code needed are blocked (in case an older environment still has them) |
| `tests/test_pluto_assets.py` | the asset finder |

Known differences, not reconciled: Pluto stamps `period_open` with the session label,
Zipline with the execution open (compared at session level); recorded variables are not
produced on the Pluto side (the example ports do not call `record()`).

The rest of `tests/` is Zipline's own suite. Most of it does not run on the fork (fixtures
break on the fork's changes); it is out of scope.

## Out of scope (not repaired, still in the tree)

- the gRPC services (`pluto/control/controllable/server.py`, `pluto/interface/`, `dev/cli.py`),
- live and paper trading, the old controller modes (`pluto/control/modes/`),
- the launcher support that only the removed window used (`pluto/control/script_handler.py`,
  `pluto/control/setup/`, `python __main__.py add-manager`) and `dev/backtester.py`.

`python __main__.py install-examples` copies the examples into the strategy store under
`~/.pluto` and ingests the test bundle.
