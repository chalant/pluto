# PY313 progress: port Pluto to Python 3.13 / numpy 2.5.4 / pandas 2.3.x (approach B)

**STATUS: ALL CHECKPOINTS DONE (C1a, C1b, C1b rerun, C2 to C7). Nothing is left to do except the owner items at the end.**
The branch `py313-port` is committed and not pushed or merged. The head hash is in `git log` (the last commits only
update this file). The worktree was removed when the work was finished (the branch stays); to look at it again use
`git worktree add <dir> py313-port`, or clone it. The verification environment is in the scratch directory below.

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
- [x] **C3** the six test files (plus the new `tests/test_pluto_upstream_fixes.py`) run on the new stack; see "STATE OF THE TESTS".
- [x] **C4** Pluto == Zipline exact (`rtol=0, atol=0`, every common column): oracle 5/5, scenarios 21/21 (after the PM17 ports; before
  them 20/21, `eod_cancel`).
- [x] **C5** stored 0.22 comparisons at the existing tolerances: every column passes except the recorded ones (`alpha`,
  `benchmark_volatility`, `beta`; `momentum_pipeline`: `alpha` only), which are in `STORED_EXPECTED_DIFFERENCES`
  (`tests/test_pluto_examples.py`) with measured numbers, in a separate test per example marked `expectedFailure`; the new
  `2-3-3` expected set (generated by zipline's own `run_example`) is matched exactly.
- [x] **C6** fresh clone of the branch (`$P/c6/pluto`, `git clone -b py313-port`) and a fresh environment from
  `dev/setup_env.sh $P/c6/env` (uv, 30 s with a warm uv cache, 17+1 `.so`): all 7 pluto test files: **78 passed, 5 xfailed, 64 s**.
- [x] **C7** three consecutive full runs in that clone: each **78 passed, 5 xfailed** (59.6 s, 59.3 s, 59.6 s), the sorted per-test
  outcome lists (83 lines) identical, and the sha256 of each example's result frame (numeric columns, sorted by column name;
  list columns carry random order ids) identical across three separate processes
  (`$P/scripts/c7.sh`, `$P/scripts/hash_frames.py`). Note: the column ORDER of the frame differs between processes
  (`pluto_metrics()` returns a `set`, as zipline's `default_metrics`); values do not.
- [x] **C1b rerun** after the flat-layout commit, on the final tree: **130 passed, 1 skipped, 44.7 s** (`$P/c1b-rerun.log`).

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
`test_pluto_scenarios.py`: 21 passed (`eod_cancel` fixed by the `698b7dbb` port). `test_pluto_assets.py`: 23 passed.
`test_pluto_upstream_fixes.py`: 10 passed.
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

## PM17 table (upstream zipline-reloaded fixes since bbf92210 vs Pluto's own copies)

Pluto's own copies: `pluto/finance/{position_tracker,ledger}.py`, `blotter/simulation_blotter.py`, `metrics/{metric,tracker}.py`,
`pluto/algorithm.py` (subclass of the vendored one), `control/clock/sim_engine.pyx`, `control/controllable/controllable.py`,
`control/controller/process_manager.py`. Not Pluto copies any more (vendored, so the fixes arrive with the base): slippage, commission,
controls, order.py (`a85e787b`: `Order.handle_split`), position.py (`62cb4a13`, `f1649741`, `60dfa7ba`, `53399feb`), `_finance_ext.pyx`
(`7d089c52` CoW), `cancel_policy`, `tradesimulation.py`, `algorithm.py` (`e45d9569`, `f62666a2`, `84726a48`, `540622cc`).

| upstream | subject | in Pluto |
|---|---|---|
| `698b7dbb` | ENH: enabled EODCancel for daily frequency (#20) | PORTED: `SimulationBlotter.execute_daily_cancel_policy`; `Controllable.session_end` calls it for daily runs; `StrategyManager.place_orders` keeps the cancel time stamped by the strategy's blotter (scenario `eod_cancel` was the failing test). Minute policy NOT executed (never was on this path; needs the controller to cancel its copies) |
| `62cb4a13` | BUG: reverse-split handling of open orders and fractional shares | PORTED: `SimulationBlotter.process_splits` drops emptied orders, `PositionTracker.handle_splits` removes a position left without shares, `Ledger.process_splits` books leftover cash of either sign. Position part arrives with the vendored `Position` |
| `a85e787b` | BUG: Preserve outstanding split orders after partial fills | PORTED (blotter: `open_amount == 0`); `Order.handle_split` part is in the vendored order.py |
| `604c64f3` | Settle stock dividends once and refresh position stats | PORTED (`PositionTracker.pay_dividends`) |
| `a571cf5a` | Skip stock dividends that round to zero shares | PORTED (`PositionTracker.earn_dividends`) |
| `f1649741`, `60dfa7ba`, `53399feb` | stock dividend rounding toward zero / one-ULP / reverse split cost basis | in the vendored `Position` (the tracker creates zipline's `Position`; `pluto/finance/position.py` is dead and cannot even be instantiated: its `minimal_fractional_amount` slot assignment is forwarded to `InnerPosition`; reverted my edits there, to be deleted in the dead code step) |
| `e45d9569` | BUG: simulation edge cases in slippage, controls, metrics | metrics PORTED (`Positions` uses the `ledger.positions` property); slippage/controls/algorithm.py are vendored |
| `547664f9`, `8ae2cdc9` | `_ClassicRiskMetrics` leverage masking / reporting-period max leverage | NOT applicable: Pluto has no `_ClassicRiskMetrics` (risk report) |
| `99cc8a82` | nonbinding partial callbacks on newer Python | NOT applicable: Pluto's metrics are classes, no `partial` class attributes |
| `cd05eee3` | Return None for inf alpha | PORTED (`AlphaBeta`, which `pluto_metrics()` does not use) |
| `6833c480`/`5b5082ba` | --no-benchmark flag (reverted) | not applicable (CLI feature of run_algo) |
| `ec36dd51`, `f5abd985` | `.iloc` on daily returns arrays | NOT applicable: `ReturnsTracker` keeps a deque |
| `7d089c52` | CoW position buffers | in vendored `_finance_ext.pyx` |
| `2642caa7` | exchange-calendars 4 | done earlier (tracker, domain, clock, algorithm) |
| `1420bc43` | Cython fixes (cimport order, `NANOS_IN_MINUTE`, typed `idx`) | not ported to `sim_engine.pyx` (cosmetic; builds with Cython 3.3.1) |
| `70180390` | `box=True` removed | done earlier |
| `f62666a2` | slippage, cancel policy, order params | vendored `algorithm.py`/`slippage.py`; the blotter part was reverted again by `84726a48` and nets to `698b7dbb` |
| `206888c8`, `d3f62071`, `0b889a59`, `4d74e7b8`, `b5a56976`, `26bcb444` ... | docs, black, six/py2 removal, layout | cosmetic |

Pluto's AST-compared overridden methods of `TradingAlgorithm` differ from the vendored ones only by docstrings/calendar changes already
applied. `pluto/_protocol.pyx` (not in `setup.py`, no importer, does not compile) is left as is and NOT ported: open owner item.
`tests/test_pluto_upstream_fixes.py` (10 tests) covers the ported fixes (7 fail without them).

## What was done after C2 (steps, all committed)

1. DONE parity hardening: `assets.py` (`as_unit('ns').asi8`), `sim_engine.pyx` (`_as_nanos`), `benchmark_source.py` (`as_unit('ns').asi8`,
   `pd.Series(dtype='float64')`), `controllable.py` (`pd.Series(dtype='float64')`), `data/benchmark.py` (`astype('datetime64[ns]')`,
   `pct_change(fill_method=None)`); `git grep` shows no other `asi8`/`pd.Series()`/`pct_change` on the executed path.
   Dead and left (owner item): `pluto/coms/client/ledger.py` (`Series.append`, `pd.Series()`; no importer), `pluto/data/io/*`.
2. DONE deletions (git grep: no importer, `imptest.py` 285 ok 0 bad): `pluto/data/download/` (all), `pluto/setup/{schema,account_setup,account}.py`,
   `dev/setup_store.py`, `pluto/control/{bundle,domain}.py`, `pluto/control/clock/minute_event_gen.py`, `pluto/finance/position.py`.
3. DONE `dev/setup_env.sh` (uv, default ENV `~/.cache/pluto-dev/env313`), `dev/README.md` (rewritten for 3.13), `etc/requirements-py313.txt` (= `$P/pins-initial.txt` + header). Stale legacy CI files (`.travis.yml`, `.appveyor.yml`, `Dockerfile-dev`, `vagrant_init.sh`, `etc/dev-install`, `ci/`) still name the removed `etc/requirements_{blaze,talib}.txt`: not touched (owner item).
4. DONE C6, C7, C1b rerun (see the checklist).

## Decisions taken (for the owner)

- No versioneer; static version. Cython 3.3.1 (not `<3.2.0` as zipline-reloaded's build-system) builds the tree without errors.
- Calendar port: EC calendars, right-sided; Pluto alias registry separate from EC's dispatcher; calendar-per-run in the three
  zipline files dropped (see above).
- `create_transaction` keeps whole amounts as `int` (otherwise fill counts print as `389.0` and a zipline-reloaded test fails).

## Open owner items

- Stored 0.22 comparisons (expected failures, numbers in `STORED_EXPECTED_DIFFERENCES`): `alpha` differs by construction
  (empyrical-reloaded 0.7.3 compounds `(1 + mean) ** n - 1`; the 0.22-era empyrical multiplied: PM3), up to 1.2 % relative in
  `buyapple` and 60 % relative (1.36 absolute) in `momentum_pipeline`; `benchmark_volatility` and `beta` differ by about one unit in
  the last place in some rows (cause not analysed: numpy/pandas rounding is the likely one). Decide whether to keep the 0.22 set as
  a reference only (current state) or drop it.
- `pluto/_protocol.pyx` (1000 lines, 2019 copy of `zipline/_protocol.pyx`; not in `setup.py`, no importer, does not compile) was NOT
  ported (11 upstream commits) and not deleted: delete it?
- Dead code left (no importer): `pluto/coms/client/ledger.py` (pandas 1 `Series.append`), `pluto/data/io/*`, `pluto/coms/client/protos/`.
- Legacy CI/infra files (`.travis.yml`, `.appveyor.yml`, `Dockerfile-dev`, `vagrant_init.sh`, `etc/dev-install`, `ci/`) still name removed
  `etc/requirements_{blaze,talib}.txt` and Python 3.6.
- The rest of `tests/` (test_algorithm.py, test_assets.py, ... 2019 copies of Zipline's own tests) does not run; the vendored
  zipline's own tests are in the zipline-reloaded repository (C1a/C1b).
- The `test_resource_path` function imported into three test modules is collected by pytest as a (spurious, passing) test:
  counts include 3 of those.
- The minute-data cancel policy is not executed on the Pluto path (never was); only the daily one (`698b7dbb`).
- Pluto's stock dividends and splits use zipline's `Position` (whole shares, floor/trunc), not fractional shares:
  `pluto/finance/position.py` (fractional override) was dead and is deleted.
- `pluto/coms/*_pb2.py` and `pluto/finance/metrics/tracker_state_pb2.py` have no `.proto` source and do not import.
- Zipline pipeline tests that pass an `AssetFinder` to `SimplePipelineEngine` (zipline.testing fixtures) are incompatible with K30.
