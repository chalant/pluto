# Cleanup progress: remove the GUI, `styx` and the ClickHouse data layer

Branch `cleanup-superseded`, started at `master` = `b2a8dfdf`. Worktree:
`/home/yves/Documents/coding-projects/pluto-cleanup` (removed at the end; the branch stays).
Not pushed, not merged.

Goal (owner decision 2026-11-15 to 2026-11-20): `gui/`, `styx/` and the ClickHouse data layer
will be superseded; remove them and every mention. The in-memory simulation path must keep
working exactly as before (35 tests). Not decided, therefore kept: the gRPC/protobuf service path
(`protos/`, `pluto/coms`, `pluto/server`, `pluto/control/controllable/server.py`, `dev/cli.py`,
`pluto/interface/`), live/paper trading, brokers, the strategy store, the Zipline fork.

## How to run

```bash
WT=/home/yves/Documents/coding-projects/pluto-cleanup
PY=/home/yves/.cache/hades-pluto-dev/env36/bin/python
# build the Cython extensions in the worktree (artefacts are git-ignored: *.so, *.c, build)
(cd $WT && $PY setup.py build_ext --inplace -j 8)
# the 35 tests; HOME per command only, never export it
(cd $WT && HOME=/home/yves/.cache/hades-pluto-dev/home-restore NOSE_PARAMETERIZED_NO_WARN=1 \
  $PY -m pytest -p no:cacheprovider -q tests/test_pluto_examples.py \
  tests/test_pluto_zipline_oracle.py tests/test_pluto_isolation.py tests/test_pluto_assets.py)
# import smoke test
(cd $WT && HOME=/home/yves/.cache/hades-pluto-dev/home-restore $PY -c "import pluto")
# install-examples into a temporary HOME (deleted afterwards)
T=$(mktemp -d /home/yves/.cache/hades-pluto-dev/cleanup/ie.XXXX); \
  (cd $WT && HOME=$T $PY __main__.py install-examples); ls $T/.pluto; rm -rf $T
# import-resolution scan (script lives outside the repo)
$PY /home/yves/.cache/hades-pluto-dev/cleanup/import_scan.py $WT OUT.json [--removed PATH ...]
```

The scan (`/home/yves/.cache/hades-pluto-dev/cleanup/import_scan.py`) parses every tracked `.py`
under `pluto zipline dev tests protos directory downloader schemas setup universe` and the
top-level `*.py`, resolves every first-party `import`/`from … import` (also inside functions)
against the files of the tree (module file, package, `.pyx`, namespace dir; for `from m import n`
also a submodule or a module-level name of `m`; star imports accepted), and writes the unresolved
`(file, target)` pairs. The first-party root names are fixed to those of `b2a8dfdf` (incl. `gui`,
`styx`), so an import of a deleted package shows up as unresolved.

## Checklist

- [x] worktree + branch `cleanup-superseded` at `b2a8dfdf`; extensions built (rc=0, 18 `.so`)
- [x] baseline: 35 tests on `b2a8dfdf` in the worktree: **35 passed** in 39.45 s
- [x] inventory (below) and baseline import scan: 64 unresolved pairs (listed below)
- [x] remove GUI (commit "cleanup: remove the GUI"): 35 passed (39.03 s); `import pluto` ok; `install-examples` rc=0
  (strategies, launchers, `assets-7.sqlite` created in a temp HOME, deleted); `__main__.py --help`: add-manager,
  add-strategy, install-examples
- [x] remove `styx` (commit "cleanup: remove styx"): 18 files; no requirement, config, script or test existed
  only for it (inventory). 35 passed (39.37 s); `import pluto` ok; `install-examples` rc=0
- [x] remove ClickHouse data layer (commit "cleanup: remove the ClickHouse data layer"): `pluto/data/equity_pricing/`,
  `pluto/data/utils.py`, `pluto/data/{pluto,vendors}_db_schema.py`, `compound_calendar.py`, the ClickHouse test;
  `ClickHouseTestBundle` and its imports (incl. the module-level `clickhouse_driver`) out of `pluto/data/bundles/test.py`;
  `assets.py` unused client import, unused lifetimes query, comments; `clickhouse-driver` out of `dev/setup_env.sh`.
  Stale `pluto/data/equity_pricing/__pycache__` deleted (an untracked dir would import as a namespace package).
  35 passed (34.17 s); `import pluto` ok; `install-examples` rc=0. Without the package: 35 passed (37.12 s) with a
  `sitecustomize.py` (`cleanup/noch/`, on `PYTHONPATH`) that makes `clickhouse_driver`, `PyQt6`, `PySide6` raise
  ImportError; importing `pluto.data.bundles.test`, `zipline.data.bundles.core`, `pluto.assets.assets`,
  `pluto.data.universes.core` under it loads none of them (`loaded: []`). Real absence: fresh environment (below).
- [x] mentions (commit "cleanup: isolation test and dev/README.md without the removed components"):
  `tests/test_pluto_isolation.py` now blocks only `clickhouse_driver`, `PyQt6`, `PySide6` (see Decisions);
  `dev/README.md` rewritten (removed vs remaining, no names of removed packages); `setup.py` extras,
  `etc/requirements*.txt`, Dockerfiles, CI had nothing to remove (inventory). 35 passed (34.93 s);
  `import pluto` ok; `install-examples` rc=0. Negative control: the test's child code run after
  `import clickhouse_driver` prints `blocked packages loaded: ['clickhouse_driver', ...]` and exits rc=1.
- [ ] import scan after: unresolved ⊆ baseline
- [ ] fresh environment from `dev/setup_env.sh` under `/home/yves/.cache/hades-pluto-dev/scratch-cleanup/`: 35 tests, `pip list` without PyQt6/PySide6/clickhouse-driver; scratch deleted
- [ ] final grep; `dev/README.md`; diff stat; worktree removed, head hash recorded

## Inventory (at `b2a8dfdf`)

### Files to delete

| Area | Path | Lines | Reason |
|---|---|---|---|
| GUI | `gui/` (52 tracked files: PyQt6 windows, docks, charts, `.ui` Designer sources, generated `ui_*.py`, `mainwindow.pyproject*`, `qtlogger.py`, `test.py` scratch) | 4063 | superseded (owner) |
| GUI | `ui_main_window.py`, `ui_plotting_control.py` (repo root; PyQt6 generator header only) | 16 | generated Qt modules, nothing imports them |
| styx | `styx/` (18 files: `controller_behavior.py`, `ingestion/` EODHD/Polygon clients, ingestion DB, caches, tasks, `data/equity_data_writer.py`, `writers.py`, `pipeline/`) | 2021 | superseded (owner); nothing outside `styx/` imports it |
| ClickHouse | `pluto/data/equity_pricing/` (`bar_writer.py`, `readers.py`, `minute_bar_reader.py`, `minute_bars_writer.py`, `__init__.py`) | 1107 | ClickHouse table DDL, readers, writers; importers: `pluto/data/bundles/test.py` (ClickHouseTestBundle only), `styx/`, the ClickHouse test |
| ClickHouse | `pluto/data/utils.py` (`clickhouse_driver.Client` factory) | 12 | importers: `pluto/assets/assets.py` (unused import), `minute_bars_writer.py`, `styx/` |
| ClickHouse | `pluto/trading_calendars/compound_calendar.py` | 9 | added in "clickhouse minute bars" (`8aee4dc7`); only importer is the ClickHouse minute reader; fails on import (K32) |
| ClickHouse/ingestion | `pluto/data/pluto_db_schema.py`, `pluto/data/vendors_db_schema.py` | 270 | added in "data ingestion" (`3d478ba7`) with the ingestion code that became `styx/`; nothing imports them; the vault lists them in the superseded ClickHouse group |
| ClickHouse | `tests/data/test_clickhouse_minute_bars.py` | 1270 | tests the ClickHouse minute reader/writer against a live server; not collectable (K32) |

### Line-level edits (files kept)

| File | Lines | Change |
|---|---|---|
| `__main__.py` | 9 `import sys`, 13 `import logbook`, 52-83 `backtester` command (lazy PyQt6/`gui` import) | delete (both imports are used only by `backtester`) |
| `.gitignore` | 82 `gui/node_modules/` | delete |
| `pluto/data/bundles/test.py` | 102-319 `ClickHouseTestBundle`; imports used only by it: `numpy`, `testfixtures.tempdirectory`, `json`, `shutil`, `sqlalchemy`, `clickhouse_driver`, `calendar_utils`, `equity_pricing.*`, `configuration` | delete; `TestBundle`, `TestBundleLoader` unchanged |
| `pluto/assets/assets.py` | 9 `from pluto.data import utils` (unused), 11-14 `_GET_ASSET_LIFETIMES` (ClickHouse `lifetimes` table query, unused), 25 and 54 ClickHouse todo comments | delete |
| `pluto/configuration.py` | 6 comment `(clickhouse, alpaca, polygon.io...)` | edit: drop clickhouse (module kept: `controllable/server.py`, `live_data_source.py` use it) |
| `pluto/data/universes/core.py` | 163-165 commented-out `ClickHouseTestBundle` | delete |
| `tests/test_pluto_examples.py` | 315-318 commented-out `ClickHouseTestBundle` | delete (no assertion touched) |
| `tests/test_pluto_isolation.py` | `BLOCKED = ('gui', 'styx', 'PyQt6', 'PySide6')`, docstring, test name | adapt: gui/styx no longer exist (see Decisions) |
| `dev/setup_env.sh` | 10 `WITH_GUI` comment, 49-51 comment, 58 `clickhouse-driver==0.2.5`, 62-64 PyQt6 block | delete/edit |
| `dev/README.md` | 5-6, 43, 60, 70-78 | rewrite for the removal |
| `RESTORE_PROGRESS.md` | many | keep (history) |

No hits in `etc/requirements*.txt`, `setup.py` (extras come from `etc/requirements_*.txt`), `setup.cfg`,
`MANIFEST.in`, `Dockerfile*`, `.travis.yml`, `.appveyor.yml`, `ci/`, `conda/` recipes, `README.rst`,
`.vscode/`, `docs/` (other than unrelated words, below).

### Hits that stay (unrelated words / binary noise)

`git grep -i -E "gui|styx|clickhouse|pyqt|pyside"` also matches: `guide`/`Guidelines`/`user-guide`
(`CONTRIBUTING.rst`, `conda/README.md`, `conda/0_sortedcontainers/meta.yaml`,
`docs/source/{development-guidelines,index,install}.rst`, `docs/source/whatsnew/1.1.1.txt`,
`zipline/pipeline/factors/technical.py` URL), `distinguish` (`pluto/_protocol.pyx`, `pluto/api.pyi`,
`zipline/_protocol.pyx`, `zipline/_version.py`, `zipline/algorithm.py`, `zipline/api.pyi`,
`versioneer.py`), base64 image data in `docs/notebooks/tutorial.ipynb` and
`zipline/examples/buyapple.ipynb` (`…GuiI…`, `…KguI…`, `…GUIG…`), compressed bytes in
`tests/resources/example_data.tar.gz` (expected results; never edited).

### Used by a removed area and by a kept area, or borderline: kept

| Item | Used by | Why kept |
|---|---|---|
| `dev/events.py` | GUI and core (`clock.py`, `controllable.py`, `simulation_loop.py`, `processes.py`, `interface/monitor.py`, `dev/strategy_store.py`) | shared |
| `pluto/configuration.py` | styx, ClickHouse bundle/test and kept `controllable/server.py` (`host`, `port`), `live_data_source.py` (alpaca, polygon.io) | shared |
| `pluto/sources/benchmark_source.py`, `pluto/data/caching.py`, `pluto/setup/{run_setup,engine_setup}.py`, `pluto/control/*`, `pluto/finance/*` | GUI and core/tests | shared |
| `AbstractControllerBehavior` hooks (`ingest_watchlist_data`, `ingest_all_data`, `update_watchlist`) | shaped by `styx/controller_behavior.py`, implemented by `SimulationControllerBehavior`, called by `Controller.clock_update` | core |
| `pluto/control/script_handler.py`, `pluto/control/setup/{setup,account}.py` | only `gui/backtesting.py` | launcher support; no Qt; not decided → owner |
| `__main__.py add-manager` | creates `~/.pluto/launchers/<name>.py`, which only the removed `backtester` command consumed | not decided → owner |
| `dev/backtester.py` | nothing (older copy of the GUI's run builder, no Qt) | not decided → owner |
| `pluto/data/data_sources/data_source.py` (`SimulationDataSource`) | built only by the GUI; module imported by `controller.py` | kept area imports it |
| `pluto/data/data_sources/live_data_source.py` | nothing (live, reads `alpaca`, `polygon.io` config) | live code, out of scope |
| `pluto/data/schema.py` | nothing; added in "backtesting ui" (`98ff800c`) together with the kept bundles | generic bundle tables, not Qt/ClickHouse |
| `tests/data/test_pluto_minute_bars.py` | empty file (0 bytes) | not ClickHouse-specific by name or content |
| `testfixtures` package | was imported by `ClickHouseTestBundle`; still imported by `zipline/testing/core.py` (used by `TestBundle`) | stays in the environment |

### Importers of removed modules (scan with `--removed`)

```
__main__.py 62 gui:backtesting
__main__.py 63 gui:qtlogger
pluto/assets/assets.py 9 pluto.data:utils
pluto/data/bundles/test.py 33 pluto.data.equity_pricing.bar_writer:ClickHouseDataWriter, :DailyBars
pluto/data/bundles/test.py 34 pluto.data.equity_pricing:bar_writer
pluto/data/bundles/test.py 35 pluto.data.equity_pricing:readers
```

Watched third-party imports: `__main__.py:61 PyQt6`; `clickhouse_driver` in
`pluto/data/bundles/test.py:19`, `pluto/data/equity_pricing/{bar_writer:3,minute_bar_reader:9,readers:6}`,
`pluto/data/utils.py:1`, `tests/data/test_clickhouse_minute_bars.py:38`. Parse error (pre-existing,
unrelated): `pluto/control/clock/domain_filter.py` line 109.

### Baseline unresolved first-party imports (64 pairs, `b2a8dfdf`)

All pre-existing: dynamic `zipline.api` names (registered at run time), old `protos` modules,
broken `pluto.coms`, `pluto.control.modes`, `pluto.data.download`, `pluto.interface`, calendars.

```
pluto/client.py protos:controller_service_pb2_grpc
pluto/coms/controllable.py protos:controllable_service_pb2
pluto/coms/controllable.py protos:controllable_service_pb2_grpc
pluto/coms/controllable.py protos:controller_service_pb2_grpc
pluto/coms/controller.py protos:controllable_service_pb2
pluto/coms/controller.py protos:controllable_service_pb2_grpc
pluto/coms/controller.py protos:controller_service_pb2
pluto/coms/controller.py protos:controller_service_pb2_grpc
pluto/coms/utils/test_cert.py pluto.coms.utils.certification:CertificateSigningRequestBuilder
pluto/coms/utils/test_cert.py pluto.coms.utils.certification:send_certificate_request
pluto/control/modes/live_mode.py pluto.control.modes.processes:process_manager
pluto/control/modes/live_simulation_mode.py pluto.broker:broker
pluto/control/modes/live_simulation_mode.py pluto.control.processes:process_manager
pluto/control/modes/market/factory.py pluto.universes:universes
pluto/control/modes/mode.py pluto.broker:broker_service
pluto/control/modes/simulation_mode.py pluto.broker:broker
pluto/control/modes/simulation_mode.py pluto.control.processes:process_manager
pluto/data/download/downloaders.py pluto.data.ingestion.ingester:DailyEquityIngester
pluto/data/download/executors/alpha_vantage.py pluto.data.download:EquityRequest
pluto/data/download/executors/quandl.py pluto.data.download:EquityRequest
pluto/data/download/executors/quandl.py pluto.data.download:_RequestExecutor
pluto/data/download/executors/wikipedia.py pluto.data.download:SandPConstituents
pluto/data/download/executors/yahoo.py pluto.data.download:EquityRequest
pluto/data/download/executors/yahoo.py pluto.data.download:_RequestExecutor
pluto/data/universes/alpaca.py pluto.data.bundles:pluto
pluto/data/universes/writer.py pluto.universes:schema
pluto/interface/directory.py pluto.setup:setup
pluto/interface/interface.py pluto.controller:controllerservice
pluto/trading_calendars/exchange_calendar_nyse.py pluto.trading_calendars.calendar_utils:TradingCalendarFactory
pluto/trading_calendars/us_holidays.py pluto.trading_calendars.protos:calendar_pb2
tests/data/bundles/test_quandl.py zipline:get_calendar
tests/history/generate_csvs.py zipline.data.us_equity_minutes:BcolzMinuteBarWriter
tests/pipeline/test_pipeline_algo.py zipline.api:attach_pipeline
tests/pipeline/test_pipeline_algo.py zipline.api:get_datetime
tests/pipeline/test_pipeline_algo.py zipline.api:pipeline_output
tests/test_live.py zipline.testing.fixtures:WithTradingEnvironment
tests/test_live.py zipline.utils.calendars.trading_calendar:days_at_time
zipline/examples/buy_and_hold.py zipline.api:order
zipline/examples/buy_and_hold.py zipline.api:symbol
zipline/examples/buyapple.py zipline.api:order
zipline/examples/buyapple.py zipline.api:record
zipline/examples/buyapple.py zipline.api:symbol
zipline/examples/dual_ema_talib.py zipline.api:order
zipline/examples/dual_ema_talib.py zipline.api:record
zipline/examples/dual_ema_talib.py zipline.api:symbol
zipline/examples/dual_moving_average.py zipline.api:order_target
zipline/examples/dual_moving_average.py zipline.api:record
zipline/examples/dual_moving_average.py zipline.api:symbol
zipline/examples/momentum_pipeline.py zipline.api:attach_pipeline
zipline/examples/momentum_pipeline.py zipline.api:order_target_percent
zipline/examples/momentum_pipeline.py zipline.api:pipeline_output
zipline/examples/momentum_pipeline.py zipline.api:record
zipline/examples/momentum_pipeline.py zipline.api:schedule_function
zipline/gens/brokers/alpaca_broker.py zipline.api:symbol
zipline/gens/brokers/ib_broker.py zipline.api:symbol
zipline/test_algorithms.py zipline.api:order
zipline/test_algorithms.py zipline.api:order_percent
zipline/test_algorithms.py zipline.api:order_target
zipline/test_algorithms.py zipline.api:order_target_percent
zipline/test_algorithms.py zipline.api:order_target_value
zipline/test_algorithms.py zipline.api:order_value
zipline/test_algorithms.py zipline.api:record
zipline/test_algorithms.py zipline.api:set_slippage
zipline/test_algorithms.py zipline.api:sid
```

## Evidence

Per step in the checklist. Scans, fresh environment and final grep: below once done.

## Decisions

- **Isolation test** kept as a child-interpreter test but its block list changed from
  `('gui', 'styx', 'PyQt6', 'PySide6')` to `('clickhouse_driver', 'PyQt6', 'PySide6')`: the two first-party
  packages no longer exist (any import of them now fails every example test), while the third-party
  packages may still be installed in an older environment (env36 has `clickhouse-driver` 0.2.5 and PyQt6
  6.2.3), so blocking them still proves the path does not import them. Cost: this one line is a residual
  hit of the final grep. Asked Main whether a name-free variant (allow-list of imported third-party
  packages, brittle across environments) is wanted instead; default kept.
- **Borderline items kept** (owner to decide): see the "Used by a removed area and by a kept area" table.
- **`defaultdict` and `os.path` imports** in `pluto/data/bundles/test.py` were already unused before the
  cleanup; left as they are (not part of the removed code).
- **Accident, repaired**: the first edit of `__main__.py` was applied with a relative path to the main
  checkout (`/home/yves/Documents/coding-projects/pluto`) instead of the worktree; that checkout was clean
  apart from its untracked folders, so `git checkout -- __main__.py` there restored it exactly
  (`git status` afterwards: only `?? notes/ tsgraph-kernel-spec/ tsgraph-spec/`, as before). All later
  edits use absolute worktree paths.

## Next step

Import scan after (`import_scan.py $WT after.json`, compare its `unresolved` pairs with the baseline list
above), then the fresh environment (`HOME=/home/yves/.cache/hades-pluto-dev/scratch-cleanup/home
dev/setup_env.sh /home/yves/.cache/hades-pluto-dev/scratch-cleanup/env36` from a fresh clone of the
branch), then the final grep and the diff stat.
