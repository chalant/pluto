# Pluto: the in-memory simulation path

Branch `py313-port` ports Pluto to **Python 3.13** (numpy 2.5, pandas 2.3, exchange_calendars 4,
SQLAlchemy 2, protobuf 7). It keeps one path of Pluto in a working, tested state: **daily
backtests of a strategy through Pluto's own controller/controllable runtime, all in one process**
(no gRPC, no database server, no network). Its results match Zipline's exactly.
`PY313_PROGRESS.md` at the repository root has the evidence, the decisions and the change list
of the port. Earlier stages: `RESTORE_PROGRESS.md` (restoration, branch `restore-sim`),
`ALIGN_PROGRESS.md` (exact alignment with Zipline), `WIDEN_PROGRESS.md` (`USEquityPricing`
support, 64-bit fields), `CLEANUP_PROGRESS.md` and `PYCLEAN_PROGRESS.md` (removed components and
files).

`zipline/` is a copy of **zipline-reloaded 3.2.1** (`zipline/VENDORED.txt`; Apache-2.0), with
Pluto's changes on top; each modified file carries a "Modified by Pluto" header line. It is not
renamed and does not track upstream. Pluto keeps its own copies of a few Zipline modules
(`pluto/finance/{ledger,position_tracker}.py`, `blotter/simulation_blotter.py`, `metrics/`,
`pluto/algorithm.py`, `pluto/control/clock/sim_engine.pyx`); the behaviour fixes of
zipline-reloaded since the 2019 base are mirrored there (table in `PY313_PROGRESS.md`; each
commit says `mirrors zipline-reloaded <commit>`).

## What runs

```
SimulationLoop (24/7 clock)                   pluto/control/loop/simulation_loop.py
  └─ Controller.clock_update                  pluto/control/controller/controller.py
       │     phases on the bus of the run (below), in lock-step
       ├─ SimulationControllerBehavior        pluto/control/controller/simulation_behavior.py
       │     last sale prices from the data portal; data hooks are no-ops
       ├─ BrokerParticipant                   pluto/control/controller/participants.py
       │   ├─ StrategyManager                 pluto/control/controller/process_manager.py
       │   │     strategy registry, broker states and orders of the strategies
       │   ├─ BrokerService                   pluto/broker_service/
       │   │     the book: ledger, blotter, positions per calendar; stream of its writes
       │   └─ MarketSimulation                pluto/finance/markets/market_simulation.py
       │         fills (slippage, commission) against the bundle
       ├─ MetricsParticipant                  pluto/control/controller/participants.py
       │     the metrics tracker, reads the ledger of the book
       └─ RuntimeEndpoint -> StrategyStub -> InMemoryControllableStub
                                              pluto/control/processes/, pluto/control/controllable/
             one Controllable per strategy: pluto TradingAlgorithm, clock, pipeline
```

`tests/test_pluto_examples.py` (helpers in `pluto/test_utils/test_utils.py`) wires it up for
the five strategies in `pluto/examples/` on the `test` bundle (`pluto/data/bundles/test.py`,
bcolz data from `tests/resources/example_data.tar.gz`).

## How a run is driven: phases on the bus

A run has one bus (`pluto/bus/`; `StrategyManager(asset_finder, stp, bus=None).bus`, an `InMemoryBus` by default, any
`Bus` can be given: the in-process wiring of the tests picks it up from the manager). The controller owns the clock and
does not call its components: at each clock event it publishes one event per phase and runs the bus until it is idle
(`pluto/control/phases.py`), then checks that every participant of the phase said `done` before it publishes the next
(lock-step; a `done` of a participant that is not expected is accepted; a missing `done`, a `done` that carries an error, a stale or doubled one, or a message the bus gave up on
raises `PhaseError`, there is no timeout). The order is the order of the calls before the bus, and the floating point
sums depend on it (transactions before commissions; `revalue` right before each metrics read; the prices of the tick
are read before its writes; with two calendars the book and metrics phases run signal after signal).

| phase (rank) | topic | published | subscribers | `done` |
|---|---|---|---|---|
| book (10) | `phase.book` | per signal (`SignalEvent`: calendar time, event, last sale prices) | broker: sync prices, match, dividends and splits, expiries, `revalue` where the tracker reads | broker |
| metrics (20) | `phase.metrics` | per signal | metrics tracker: reads the ledger (`end_of_bar`, `start_of_session`, `end_of_session`) | metrics |
| account (30) | `phase.account` (`AccountEvent`) | per tick | broker: sends each strategy its `BrokerState` (fills, last sale prices) on `strategy.account.<process id>`; the runtime applies it | broker, each runtime |
| trade (40) | `phase.trade` (`ClockEvent`) | per tick | the strategies' runtimes run the clock event and send `WatchList` and `OrderBatch` on `strategy.watchlist` and `strategy.orders` (key: process id), taken by the strategy manager | each runtime: the number of orders it sent; the broker side checks it against the orders it received, per strategy, in the submit phase |
| submit (50) | `phase.submit` (`ClockEvent`) | per tick | broker: submits the orders of the tick to the book, starts the next tick | broker |

Messages carry `logical_ts` = the clock time of the tick in nanoseconds and `phase` = the rank; a `done` (topic
`phase.done`, `PhaseDone`) has the rank plus one. Every payload is a protobuf message (`protos/phases.proto` and the
existing `ClockEvent`, `BrokerState`, `OrderBatch`, `WatchList`), so a `SerializingBus(inner, phases.codec())` carries
a whole run through bytes with identical results (`tests/test_pluto_phases_run.py`). A participant's failure is caught
where it happens, logged with its traceback and reported in its `done` (the bus redelivers a handler that raises, and a
phase is not idempotent). What stays direct, in process: the controller's behavior reads the book's positions to
price them, and the metrics tracker reads the ledger object.

The runtimes must run their clock event synchronously (`running_behavior.Blocking`): their batch must be on the bus when
the trade phase ends, which is what the in-memory stub does. The bus logs every message (retention is open in
`STAGE1_BUS_PROGRESS.md`).

## Strategy scripts (interim convention)

There is no strategy database. A run's strategy script is the file
`<root>/strategies/<strategy_id>.py`: `ControllableService.Initialize`
(`pluto/control/controllable/server.py`) reads it, and the run's settings live in a
`StrategySetup` object (`pluto/strategies/setup.py`: `look_back`, `cancel_policy`,
`shorting`, `asset_classes`, `data_frequency`, `fractional`). `<root>/strategies/states` and
`<root>/strategies/performance` hold the saved state and the performance packets.
`pluto.setup.directory_setup.create_directory(root)` creates the layout, and
`directory_setup.add_strategy_script(root_dir, name, source_path)` copies a script into place
(`FileExistsError` if the file exists); `TestClient.add_strategy` and `install-examples` use
it. The strategy store concept (names, metadata, sessions, status, templates) will be
redesigned.

## Environment

Python 3.13 (tested with 3.13.16) with the exact versions of `etc/requirements-py313.txt`
(`etc/requirements.txt` has the loose requirements). Create it with
[`uv`](https://docs.astral.sh/uv/) and build the Cython extensions:

```bash
dev/setup_env.sh ~/.cache/pluto-dev/env313        # uv venv + uv pip sync + build_ext --inplace
```

A C compiler is needed (`bcolz-zipline` has no wheel for 3.13 and builds from its sdist; the
extensions are built in place). `numpy 2.5.4`, `polars 2.1.0`, `pyarrow 26.0.1`, `blake3 1.0.11`
and `duckdb 2.0.0` are styxlib's exact pins; pandas stays on 2.3.x. Rebuild the extensions after
changing a `.pyx`: `python setup.py build_ext --inplace -j 8`. After changing a `.pxd`, add
`--force`: the build does not see that another extension `cimport`s it (for example
`zipline/finance/_finance_ext.pyx` uses `InnerPosition` from `zipline/_protocol.pxd`).

The build needs Cython 3 (`language_level=3`; Cython 3.3.1 builds the tree without errors).
There is no versioneer: the version is static (`0.1.0.dev0`).

The old Python 3.6 / pandas 0.22 environment (conda-forge through micromamba) is no longer
created by `dev/setup_env.sh`; `RESTORE_PROGRESS.md` describes it.

### Protocol buffers

The `protos/*_pb2.py` modules are generated with the environment's `grpcio-tools` (1.85.3,
protobuf 7.36.2), **one `.proto` file at a time**, from the repository root:

```bash
$ENV/bin/python -m grpc_tools.protoc -I. --python_out=. [--grpc_python_out=.] protos/<name>.proto
```

(imports in the `.proto` files are relative to the root: `import "protos/<name>.proto"`;
`--grpc_python_out` only for a `.proto` that has a service and already has a `_pb2_grpc.py`).
Each of the 27 `.proto` files compiles on its own; they cannot all be compiled in one `protoc`
call, because files without a `package` define the same names (`admin`, `login`, `interface`:
`LoginRequest`; `hub`, `interface`: `Hub`; `interface`, `monitor`: `Monitor`, `Packet`, ...),
and for the same reason these modules cannot be imported together in one process.
Every non-integer numeric field is `double`, and amounts (`Position.amount`) too: a 32-bit
`float` changed limit and stop prices, the strategy's view of its portfolio and the tick
rounding, and an integer amount cannot hold a fractional position. Integer fields are only
identifiers, counts, calendar values and flags. In memory, a position's amount
(`zipline/_protocol.pxd` `InnerPosition.amount`) is `float64` for the same reason.

Saved state written before the 64-bit change cannot be read. The ledger state
(`ledger_state.proto`, inside the metrics tracker's `TrackerState`) carries a format
`version`; `Ledger.restore_state` refuses any other version with a `ValueError`.

No configuration file is needed. `~/.pluto_config.toml` (or the file named by
`PLUTO_CONFIG`) is read if present; only the live data source (Alpaca, Polygon) and the
gRPC controllable server use it.

### Calendars and time zones

Calendars are `exchange_calendars` 4 calendars, right-sided, as in zipline-reloaded.
`pluto/trading_calendars/calendar_utils.py` has `get_calendar_in_range(name, start, end, cache)`
(a calendar bounded to the run) and `get_calendar(name)` (the instance cached by the latest
`cache=True` call). Exchange aliases are Pluto's own registry (`register_alias`,
`deregister_alias`, `resolve_alias`; `YAHOO` -> `XNYS` for the test bundle), not
exchange_calendars' global dispatcher. **Sessions and asset dates are time-zone naive**
(exchange_calendars' labels); clock events, minutes and `dt` are UTC aware; the conversions
are at the boundaries (`SimulationParameters`, `pluto.coms.utils.conversions`,
`auto_close_date.tz_localize(dt.tzinfo)`).

## Tests

```bash
ENV=~/.cache/pluto-dev/env313
$ENV/bin/python -m pytest -p no:randomly tests
```

| File | Checks |
|---|---|
| `tests/test_pluto_examples.py` | each example through Pluto, compared exactly with the results generated on this stack by zipline's own run path (`example_data/expected_perf/2-3-3`, key = pandas version) |
| `tests/test_pluto_zipline_oracle.py` | each example through the vendored Zipline (`zipline.examples.run_example`) and through Pluto in the same process, with the same benchmark returns; every common column, exactly |
| `tests/test_pluto_scenarios.py` | the strategies in `tests/pluto_scenarios/` (limit, stop and stop-limit orders, targets, shorts, cancellation, partial fills, commission models, schedule rules and calendars, history, pipeline screen, a pipeline on `USEquityPricing` with a `CustomFactor`, leverage, splits, dividends, delisting, the strategy's portfolio view) through plain Zipline (`run_algorithm`) and Pluto; every common column, exactly, and no column on one side only |
| `tests/test_pluto_isolation.py` | the examples run in a child interpreter in which the third-party packages that only removed code needed are blocked by a `find_spec` finder; a negative control proves that the finder blocks |
| `tests/test_pluto_assets.py` | the asset finder |
| `tests/test_pluto_state.py` | the metrics tracker's saved state (ledger, positions, orders) with fractional amounts and prices not representable in 32 bits: restored bit for bit and written again identically; state from the 32-bit modules refused; the monitor's performance packet round trip |
| `tests/test_pluto_upstream_fixes.py` | the zipline-reloaded fixes mirrored into Pluto's own copies (splits, stock dividends, daily cancel policy) |
| `tests/test_pluto_order_ids.py` | the deterministic order ids of the strategy's blotter: the process id as namespace plus the sequence of the orders placed, runs and strategies not confused, no number for an order of no shares, a given id kept, a rebuilt blotter continuing from a checkpointed sequence, compact alphanumeric ids |
| `tests/test_pluto_broker_service.py` | the broker service's stream of the strategy's book: folding it rebuilds the engine's ledger state byte for byte on every example and scenario; duplicate and cancelled orders, revaluation, sequence gaps, two calendars, state versions |
| `tests/test_bus_contract.py` | the bus contract (rules S1 to S14) on `InMemoryBus` and on `SerializingBus(InMemoryBus)`, including the delivery trace under different `PYTHONHASHSEED`s |
| `tests/test_bus_memory.py` | what holds for the in-memory bus only: bounded queues and overflow, log positions, group assignment |
| `tests/test_bus_serializing.py` | what only the serialising wrapper does: bytes on the inner bus, codecs, undecodable payloads |
| `tests/test_pluto_phases.py` | the controller's lock-step (`PhaseDriver`) on a bare bus with fake participants: a missing, failed, stale or doubled `done`, a message the bus gave up on; the phase ranks; every payload through a `SerializingBus` |
| `tests/test_pluto_phases_run.py` | whole runs driven by phases: `buy_and_hold`, `momentum_pipeline` and two scenarios give the same frames over `InMemoryBus` and `SerializingBus(InMemoryBus)`; the messages of a run and the order counts of its `done`s; a failing strategy aborts the run with a `PhaseError` |

Tolerance policy: **Pluto against the vendored Zipline is exact** (`rtol=0, atol=0`), on every
common column of the five examples and of the scenarios; against the stored expected set
(`2-3-3`) too. No tolerance or assertion is ever loosened. The stored pandas 0.22 results were
dropped from the tarball (PM24; the measured differences are recorded in
`LEFTOVERS_PROGRESS.md`). All columns are compared, `positions`, `orders` and
`transactions` included (`pluto.test_utils.test_utils.compare_column`: lists of dicts compared
after sorting, identifiers left out, same keys). Zipline reports NaN where Pluto's tracker reports
0 (first-day volatility and ratios); NaN in the reference counts as 0. The stored pickles are
never edited; `tests/resources/add_expected_perf.py` adds the set of the pandas version in use to
the tarball.

Known differences, not reconciled: recorded variables are not produced on the Pluto side
(the example ports do not call `record()`; the controller-side tracker has no channel for
them). The cancel policy of minute runs is not executed on the Pluto path (the daily one is).
Fractional trading is not reachable on the in-memory path (the controllable's
`StrategySetup.fractional` is always `False`, and Zipline casts order amounts to integers,
so there is no oracle); only saved state is tested with fractional positions.

Pipelines may use the generic `EquityPricing` columns or the US ones (`USEquityPricing`):
the universe's loader serves `EquityPricing` specialised to any domain of its country, and
the engine runs a `USEquityPricing` pipeline on `US_EQUITIES`, as Zipline does.

`tests/` holds only Pluto's suite. The 2019 copy of Zipline's own tests that was there was
deleted (PM26; each file is recorded in `LEFTOVERS_PROGRESS.md`); the tests of the vendored
`zipline/` are those of zipline-reloaded 3.2.1 (`tests/test_examples.py`,
`tests/finance/test_transaction.py`, `tests/test_algorithm.py` and its pipeline tests were run
against the vendored package, see `PY313_PROGRESS.md` and `LEFTOVERS_PROGRESS.md`).

## Out of scope (not repaired, still in the tree)

- the gRPC services (`pluto/control/controllable/server.py`, `pluto/interface/`, `dev/cli.py`),
- live and paper trading.

`python __main__.py install-examples` (from the repository root) copies the examples to
`~/.pluto/strategies/<name>.py` (existing files are kept) and ingests the test bundle.
