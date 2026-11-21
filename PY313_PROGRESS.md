# PY313 progress: port Pluto to Python 3.13 / numpy 2.5.4 / pandas 2.3.x (approach B)

Branch `py313-port`, worktree `/home/yves/Documents/coding-projects/pluto-py313`, based on `master`
(`2de37970` after the history rewrite; Zipline base in this repository is `72fdeb44`, upstream `bbf92210`).
Not pushed, not merged. Hashes: use `git log` (the lead re-parented the branch once; never record pre-rewrite hashes).
Scratch: `/home/yves/.cache/hades-pluto-dev/py313/port/` (`env313` = Python 3.13.16 venv built with
`uv pip sync pins-initial.txt` = `PyDeps/res_b_pandas_lt3.txt`; `zr` = zipline-reloaded clone at tag 3.2.1;
logs `c1-plain.log`, `c1-pluto.log`, `build*.log`; scripts `imptest.py`, `t.sh`).
Studies (read-only): `/home/yves/.cache/hades-pluto-dev/py313/{PyPort,PyDeps,PyScan}/REPORT.md`.

## RULES that bit me (read first)

- The tools' default cwd is the OWNER'S MAIN CHECKOUT (`/home/yves/Documents/coding-projects/pluto`). Always use absolute
  paths into the worktree (`read`, `edit`, `bash`) and check `git rev-parse --show-toplevel` before committing.
- Never run two git commands in parallel (index.lock). `HOME` per command, never `export`.
- The `edit` tool needs the file tag from a `read` of the exact lines; a first edit often fails with "never displayed":
  re-send the same edit and it works.

## How to build and test (new stack)

```bash
WT=/home/yves/Documents/coding-projects/pluto-py313
P=/home/yves/.cache/hades-pluto-dev/py313/port
(cd $WT && HOME=$P/home $P/env313/bin/python setup.py build_ext --inplace -j 8)   # ~35 s, forces cythonize
$P/t.sh -x $WT/tests/test_pluto_examples.py             # pytest wrapper (HOME, cache dir, no warnings, short tb)
HOME=$P/home $P/env313/bin/python $P/imptest.py $WT      # C2 import check of PyScan's critical path
# zipline-reloaded's own tests against the vendored package (C1):
(cd $P/zr && PYTHONPATH=$WT HOME=$P/home $P/env313/bin/python -m pytest -o cache_dir=$P/pytestcache -p no:randomly \
   tests/test_examples.py tests/finance/test_transaction.py tests/test_algorithm.py -k 'not talib' -q)
```

(Do not pass `-p no:cacheprovider` to the zr tests: their `addopts` has `--ff`.) Old-stack reference runs of
`origin/master` only: see the brief (`env36`).

## Checklist with evidence

- [x] S1 vendoring: commit "Base: zipline-reloaded 3.2.1 (tag 3.2.1), Apache-2.0" (`zipline/` = `src/zipline` of the tag,
  `zipline/VENDORED.txt`; `zipline/__init__.py` upstream's, PM15). Pluto's `zipline/` is gone, so the 24 dead and 9
  cosmetic changes are dropped.
- [x] Build commit: `setup.py` rewritten (3.2.1 extension list on the flat layout + `pluto.control.clock.sim_engine`, no
  duplicate int64 window (K83), no distutils/versioneer, `language_level="3"`, static version `0.1.0.dev0`);
  `etc/requirements.txt` loose modern set; `etc/requirements_{blaze,ib,alpaca,build,talib}.txt` removed (PM13).
- [x] **C1a** plain vendored build: Cython 3.3.1, numpy 2.5.4, setuptools 84.2.1, 17 `.so`, 35 s, 0 "error" lines;
  zipline-reloaded `tests/test_examples.py`, `tests/finance/test_transaction.py`, `tests/test_algorithm.py`
  (`-k 'not talib'`): **130 passed, 1 skipped, 41.75 s**.
- [x] Re-applied Pluto changes (one commit each, each names the Pluto commit(s), "originally against zipline bbf92210", K-number):
  K77 float64 `InnerPosition.amount`; fractional `create_transaction` (whole amounts stay `int`; upstream's
  `py['commission'] = None` kept); `sqlite3.connect(check_same_thread=False)`; K1 `FractionalEquity`;
  `Positions(OrderedDict)`; `ffill and field == "price"`; `DispatchBarReader` compares `calendar.name`; K28
  `Order(session_id, ..., id_)` + `hash/eq` by id, `Blotter.order`/`SimulationBlotter.order(session_id="")`
  (keeps zipline-reloaded `62cb4a13` split fix); K30 `SimplePipelineEngine(controllable)` + algorithm passes `self`;
  K65 `before_trading_start(algo, data)`, `tradesimulation`, `api_support` (registers on `pluto.api` and `zipline.api`);
  `MetricsTracker(ledger=None)`; `bundles.core.load` builds `pluto.assets.assets.AssetFinder` (K58) and exposes
  `most_recent_data` at module level (keeps `6a940fd5`); `zipline.testing.core.zipline_reloaded_git_root` for the flat layout.
  Each modified file carries a "Modified by Pluto" header line.
  **Not re-applied (decisions):** `bcolz_daily_bars.py`, `bcolz_minute_bars.py`, `pipeline/domain.py` calendar-per-run
  (upstream's code kept: EC caches instances per (name, start, end) and `DispatchBarReader` compares names; removes the
  zipline->pluto import in those files); `_calls` slot, `== 0.0`, `FROM splits` case, `embedsignature(True)`,
  `__unicode__`, `allowed_only_in_before_trading_start`, the `_ensure_ctable` drop (all cosmetic/dead/buggy).
- [x] **C1b** same three zipline-reloaded test files against the vendored package with all re-applied changes:
  **130 passed, 1 skipped, 37.8 s** (`c1-pluto.log`). (First run had 1 failure from float fill counts "389.0 shares";
  fixed by keeping whole amounts `int` in `create_transaction`.) Rerun once more at the end (flat-layout path commit).
- [x] Protobuf: every `protos/*_pb2.py` regenerated one `.proto` at a time (`python -m grpc_tools.protoc -I. --python_out=.
  [--grpc_python_out=.] protos/<n>.proto`, grpcio-tools 1.85.3 / protobuf 7.36.2; gRPC stubs where one existed and the
  proto has a service; `admin_pb2`/`login_pb2` not added). All 25 `_pb2` and 9 `_pb2_grpc` modules in `protos/` import
  **each on its own** (checked one process per module). `controller_pb2_grpc.py`, `data_bundle_pb2_grpc.py` are stale
  (their `.proto` has no service) but import. Not regenerated, no `.proto` source, do not import (no importer):
  `pluto/coms/{data_bundle,hub}_pb2*.py`, `pluto/coms/client/protos/account_state_pb2.py`,
  `pluto/finance/metrics/tracker_state_pb2.py`. Modules known to clash if imported together (not checked together):
  `admin`/`login`/`interface`, `hub`/`interface`, `interface`/`monitor`.
- [x] **C2** `imptest.py`: **285 of the critical-path modules that still exist import in one process, 0 bad** (11 of
  PyScan's list are deleted or renamed: strategy store, `wrappers.py`, zipline modules gone in 3.2.1).
- [~] C3 (agent 2, in progress): all six test files now collect and run on the new stack (see "STATE OF THE TESTS" below).
- [ ] C4 Pluto == Zipline exact (oracle: 5/5 pass; scenarios: 20/21 pass, `eod_cancel` fails = upstream `e45d9569` cancel policy, PM17);
  C5 stored comparisons (done, expected failures recorded); C6 fresh clone; C7 three runs.

## Ported so far in `pluto/` (S2)

- `pluto/trading_calendars/calendar_utils.py` rewritten on exchange_calendars: `get_calendar_in_range(name, start, end,
  cache)` (right-sided, tz-aware inputs become naive midnight labels via `to_session_label`, default end = start+10d),
  `get_calendar(name)` (cached or `RuntimeError`), Pluto-owned alias registry `register_alias/deregister_alias/resolve_alias`
  (`YAHOO` -> `XNYS`; NOT registered in EC's global dispatcher, where zipline's examples register a `YAHOO` instance).
  Deleted: Quantopian dispatcher copy, `wrappers.py`, proto `TradingCalendar`, `exchange_calendar_nyse.py`, `us_holidays.py`.
- Timezone policy (follows zipline-reloaded): **sessions and asset dates are naive**; clock events (`sim_engine.pyx` yields
  `pd.Timestamp(nanos, tz='UTC')`), minutes and `dt` are UTC-aware. Boundaries convert: `SimulationParameters` start,
  `to_zp_asset` dates (`conversions._to_asset_date`), `auto_close_date.tz_localize(dt.tzinfo)` (controllable, process_manager),
  `_execution_open_and_close`.
- `pluto/api.py`: `__all__ = []` first (zipline's eager `__init__` imports `zipline.algorithm` whose `api_method` registers there).
- `pluto/algorithm.py` (`collections.abc`, no `normalize_date`/`clear_dataframe_indexer_caches`, `trading_calendar` property,
  `calendar.sessions`), `process_manager.py` (`cu.resolve_alias` instead of the private `_default_calendar_aliases`),
  `universes/core.py` (`cu.register_alias('YAHOO','XNYS')`), `clock/utils.py` (`execution_times`), `sim_engine.pyx` (no `box=True`,
  `_as_nanos` explicit ns), `assets/assets.py` (SQLAlchemy 2, `as_unit('ns')`), `data/benchmark.py` (`ZiplineBenchmark` reads
  `<ZIPLINE_ROOT>/data/<ticker>_benchmark.csv`, else zipline's checked-in SPY file, `float_precision='round_trip'` = equals the
  stored results; `pct_change(fill_method=None)`), `benchmark_source.py`, `controllable.py` (`pd.Series(dtype='float64')`),
  `pipeline/domain.py` (`sessions()`, `first_minutes`), `metrics/tracker.py` (mirrors `2642caa7`), `data/bundles/test.py`
  (`bcolz_minute_bars`, `tarfile filter='data'`), `commands.py` (`_flag` slot: `abc.ABC` has `__slots__` on 3.13).
- Test helpers: `test_utils.stored_with_utc_timestamps` (0.22 pickles load nested Timestamps naive under pandas 2: localised to UTC
  for the comparison); the old "drop stored `commission: None`" is gone (the vendored `Transaction.to_dict` emits it).
- `tests/test_pluto_examples.py`: `parameterized`, `contextlib`, `tarfile filter`, `STORED_PANDAS_VERSION='0-22-0'`,
  `NEW_PANDAS_VERSION`, aliases through `cu`. `tests/test_pluto_state.py`: `get_calendar` from zipline, naive `first_trading_day`.

## STATE OF THE TESTS (agent 2)

`t.sh tests/test_pluto_examples.py`: 11 passed (incl. the spurious collected import `test_resource_path`), 5 xfailed.
`test_pluto_zipline_oracle.py`: 6 passed (5 examples exact vs zipline's own `run_example`, same benchmark returns).
`test_pluto_scenarios.py`: 20 passed, 1 FAILED `eod_cancel` (orders column: zipline reports the order cancelled at EOD
again on a later day; Pluto's `simulation_blotter` lacks upstream `e45d9569` cancel policy). `test_pluto_assets.py`: 23 passed.
`test_pluto_isolation.py` (find_spec blocker + negative control) and `test_pluto_state.py`: 7 passed.

Examples test design: `test_simulation_*` = Pluto vs the NEW expected set `2-3-3` (generated by
`tests/resources/add_expected_perf.py` with zipline's own `run_example`, same benchmark returns; exact);
`test_stored_results_*` = Pluto vs stored `0-22-0` on every column NOT in `STORED_EXPECTED_DIFFERENCES` (existing tolerance);
`test_stored_results_expected_differences_*` = the differing columns at the existing tolerance, `unittest.expectedFailure`.
Measured numbers (rows differing, rows, max abs, max rel) are in `STORED_EXPECTED_DIFFERENCES` in `tests/test_pluto_examples.py`.
`tests/resources/example_data.tar.gz` now also holds `expected_perf/2-3-3/` (all original members kept).

Fixes made at the root: `zipline/data/dispatch_bar_reader.py` `AssetDispatchSessionBarReader.sessions` clips to the run calendar
(exchange_calendars raises outside its range; trading_calendars clipped); `pluto/examples/momentum_pipeline.py` `Index | Index` ->
`.union` (pandas 2 made `|` logical, as upstream's example); `ZiplineBenchmark.read` public; `_run`/`StrategyTester.run` no longer take or
validate expected results (`test_utils.frame_errors` replaces `TestMetricsTracker.validate`).
Scratch script: `$P/scripts/measure.py` (differences vs stored).

## EXACT NEXT STEPS

1. PM17: port upstream fixes into Pluto's own copies (`pluto/finance/ledger.py`, `position_tracker.py`, `blotter/simulation_blotter.py`,
   `metrics/*`, `pluto/control/clock/sim_engine.pyx`, `pluto/algorithm.py`); make `eod_cancel` pass; table for the report.
2. Parity hardening (ns units), deletions (PM23 + `minute_event_gen.py`), `dev/setup_env.sh` + `dev/README.md` + `etc/requirements-py313.txt`.
3. C6 fresh clone + env; C7 three runs; C1b rerun; remove worktree only when all committed.

## Decisions taken (for the owner)

- No versioneer; static version. Cython 3.3.1 (not `<3.2.0` as zipline-reloaded's build-system) builds the tree without errors.
- Calendar port: EC calendars, right-sided; Pluto alias registry separate from EC's dispatcher; calendar-per-run in the three
  zipline files dropped (see above).
- `create_transaction` keeps whole amounts as `int` (otherwise fill counts print as `389.0` and a zipline-reloaded test fails).

## Open owner items

- Stored 0.22 comparisons: `alpha` differs by construction (empyrical-reloaded compounds, PM3); `benchmark_volatility`/`beta`
  differ by 1 ulp in some rows (numpy/pandas std/cov rounding); they stay at the existing tolerances and become expected
  failures with measured numbers (not yet written).
- `pluto/coms/*_pb2.py` and `pluto/finance/metrics/tracker_state_pb2.py` have no `.proto` source and do not import.
- Zipline pipeline tests that pass an `AssetFinder` to `SimplePipelineEngine` (zipline.testing fixtures) are incompatible with K30.
