# Pluto: the in-memory simulation path

This branch (`restore-sim`, from `origin/dev` `816ecbc2`) restores one path of Pluto to a
working, tested state: **daily backtests of a strategy through Pluto's own
controller/controllable runtime, all in one process** (no gRPC, no database server, no
network). Its results match Zipline's exactly. `RESTORE_PROGRESS.md` at the repository root
has the evidence, the decisions and the full change list of the restoration;
`ALIGN_PROGRESS.md` those of the exact alignment (branch `align-exact`), `WIDEN_PROGRESS.md`
those of `USEquityPricing` support and the 64-bit fields (branch `usequity-widen`).

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
changing a `.pyx`: `python setup.py build_ext --inplace`. After changing a `.pxd`, add
`--force`: the build does not see that another extension `cimport`s it (for example
`zipline/finance/_finance_ext.pyx` uses `InnerPosition` from `zipline/_protocol.pxd`) and
would keep a module compiled against the old layout. A checkout built before branch
`usequity-widen` (where `InnerPosition.amount` became `float64`) needs this once.

The `protos/*_pb2.py` modules are generated. After changing a `.proto`, regenerate the
module with the environment's `grpcio-tools` (1.33.2), from the repository root:
`$ENV/bin/python -m grpc_tools.protoc -I. --python_out=. protos/<name>.proto`
(imports in the `.proto` files are relative to the root: `import "protos/<name>.proto"`).
Every non-integer numeric field is `double`, and amounts (`Position.amount`) too: a 32-bit
`float` changed limit and stop prices, the strategy's view of its portfolio and the tick
rounding, and an integer amount cannot hold a fractional position. Integer fields are only
identifiers, counts, calendar values and flags. In memory, a position's amount
(`zipline/_protocol.pxd` `InnerPosition.amount`, used by Zipline's and Pluto's positions) is
`float64` for the same reason.

Saved state written before this change cannot be read. The ledger state
(`ledger_state.proto`, inside the metrics tracker's `TrackerState`) carries a format
`version`; `Ledger.restore_state` refuses any other version with a `ValueError` (old state
has none: its 32-bit values would otherwise parse as unknown fields, silently 0). The other
messages with widened fields are not restored from storage on the kept path
(`Controllable.restore_state` raises `NotImplementedError`; no code in the tree writes the
slippage parameter files that `pluto/finance/slippage/setup.py` reads): any such file
written with the old modules would lose its values.

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
    tests/test_pluto_scenarios.py \
    tests/test_pluto_state.py
```

| File | Checks |
|---|---|
| `tests/test_pluto_examples.py` | each example through Pluto, compared with the stored Zipline results on every column both have (`TestMetricsTracker.validate`) |
| `tests/test_pluto_zipline_oracle.py` | each example through the fork's plain Zipline (`zipline.examples.run_example`) and through Pluto in the same process; every common column, exactly |
| `tests/test_pluto_scenarios.py` | the strategies in `tests/pluto_scenarios/` (limit, stop and stop-limit orders, targets, shorts, cancellation, partial fills, commission models, schedule rules and calendars, history, pipeline screen, a pipeline on `USEquityPricing` with a `CustomFactor`, leverage, splits, dividends, delisting, the strategy's portfolio view) through plain Zipline (`run_algorithm`) and Pluto; every common column, exactly, and no column on one side only |
| `tests/test_pluto_isolation.py` | the examples run in a child interpreter in which the third-party packages that only removed code needed are blocked (in case an older environment still has them) |
| `tests/test_pluto_assets.py` | the asset finder |
| `tests/test_pluto_state.py` | the metrics tracker's saved state (ledger, positions, orders) with fractional amounts and prices not representable in 32 bits: restored bit for bit and written again identically; state from the 32-bit modules refused; the monitor's performance packet round trip |

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
them). Fractional trading is not reachable on the in-memory path (the controllable's
`StrategySetup.fractional` is always `False`, and Zipline casts order amounts to integers,
so there is no oracle); only saved state is tested with fractional positions.

Pipelines may use the generic `EquityPricing` columns or the US ones (`USEquityPricing`):
the universe's loader serves `EquityPricing` specialised to any domain of its country, and
the engine runs a `USEquityPricing` pipeline on `US_EQUITIES`, as Zipline does.

The rest of `tests/` is Zipline's own suite. Most of it does not run on the fork (fixtures
break on the fork's changes); it is out of scope.

## Out of scope (not repaired, still in the tree)

- the gRPC services (`pluto/control/controllable/server.py`, `pluto/interface/`, `dev/cli.py`),
- live and paper trading, the old controller modes (`pluto/control/modes/`),
- the launcher support that only the removed window used (`pluto/control/script_handler.py`,
  `pluto/control/setup/`, `python __main__.py add-manager`) and `dev/backtester.py`.

`python __main__.py install-examples` copies the examples into the strategy store under
`~/.pluto` and ingests the test bundle.
