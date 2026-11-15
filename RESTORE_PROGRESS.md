# Restore progress: in-memory simulation path on `origin/dev`

Branch `restore-sim`, started at `origin/dev` = `816ecbc2`. Worktree:
`/home/yves/Documents/coding-projects/pluto-restore`. Not pushed, not merged.

Goal (owner, 2026-11-15): the example backtests run through Pluto's own
controller/controllable path in memory, and their results match Zipline's
(a) stored expected results in `tests/resources/example_data.tar.gz`,
(b) an oracle that runs the same example through plain Zipline in the same
environment. Out of scope (do not repair, do not delete): `gui/`, `styx/`,
gRPC services (`ControllableService`, `dev/cli.py`, `pluto/interface/`),
live/paper, the ClickHouse data layer, upgrading Python/pandas.

## How to run

```bash
# environment (once): Python 3.6.15 + pandas 0.22.0, builds the Cython extensions in place
dev/setup_env.sh ~/.cache/hades-pluto-dev/env36     # existing env on this machine
# rebuild extensions only
~/.cache/hades-pluto-dev/env36/bin/python setup.py build_ext --inplace -j 8
# tests (the example tests use fresh temp dirs; HOME only matters for ~/.pluto_config.toml)
export NOSE_PARAMETERIZED_NO_WARN=1
~/.cache/hades-pluto-dev/env36/bin/python -m pytest -p no:cacheprovider \
    tests/test_pluto_examples.py tests/test_pluto_zipline_oracle.py \
    tests/test_pluto_isolation.py tests/test_pluto_assets.py
```

No `~/.pluto_config.toml` is needed for the in-memory path (see M1).

## Checklist

- [x] **M1 Baseline**
  - [x] worktree + branch `restore-sim` at `816ecbc2`
  - [x] Cython build fix (`zipline/assets/_assets.pyx` empty `FractionalEquity`) — `setup.py build_ext --inplace` rc=0, 18 `.so`
  - [x] `tests/__init__.py` no longer imports `zipline.setup/teardown` — pytest collects
  - [x] config file optional (`pluto/configuration.py`, `PLUTO_CONFIG` override) — tests ran with a HOME without the file
  - [x] `dev/setup_env.sh` (exact versions and pins)
  - [x] baseline recorded (below)
- [x] **M2** controller/behaviour refactor completed for simulation, in memory
  - [x] `AbstractControllerBehavior` declares what `Controller.clock_update` calls; `SimulationControllerBehavior` implements the hooks (no-ops)
  - [x] `test_utils.StrategyTester.run(..., controller_behavior, capital)`; the test builds the behaviour with the data portal
  - [x] `set_algo_instance` in `Controllable.before_trading_starts` and `bar`
  - [x] evidence: 4/5 pass (Blocking); same 4/5 with `NonBlocking` (worker thread); without the `set_algo_instance` fix NonBlocking raises `RuntimeError: pluto api method set_watchlist must be called during a run.` and hangs (timeout 120 s)
- [x] **M3** alignment: 5/5 vs stored results; `momentum_pipeline` alpha root cause; Zipline-vs-Pluto oracle
  - [x] alpha root cause found (empyrical version, see Evidence) → env pinned to `empyrical==0.5.2`
  - [x] zipline's own run path restored (needed for the oracle), 6/6 zipline examples equal the stored results on every numeric column
  - [x] 5/5 Pluto examples pass (`pytest tests/test_pluto_examples.py`, 6 consecutive runs + 15 runs with PYTHONHASHSEED=1..15)
  - [x] intermittent failure explained: fixed `~/pluto_tmp` kept the strategy store of an interrupted run → test now uses fresh temp dirs
  - [x] oracle `tests/test_pluto_zipline_oracle.py`: 5/5 pass, 37 common columns per example. On the way it found
    and the branch fixes: placed orders missing from Pluto's daily `orders` (controller ledger never got
    `process_order` at placement), cancelled orders with `dt=None` (controller blotter date never set),
    universe cache shared across root directories (stale readers → zipline loader tried a download).
    Not reconciled, compared at session level: `period_open` (Pluto stamps the session label, zipline the
    execution open; the execution-open code is commented out in Pluto's trackers). Not comparable:
    recorded variables (Pluto's example ports have `record()` commented out; no recorded-vars channel on
    the controller side). Reference used: the fork's own zipline (`zipline/`), restored; upstream
    `bbf92210` was not needed because the fork's zipline reproduces the stored results exactly.
  - [x] `pytest tests/test_pluto_examples.py tests/test_pluto_zipline_oracle.py`: 10 passed in one session
- [x] **M4** Pluto test files classified / passing (table in Evidence); Pluto suite: **35 passed**
  (`tests/test_pluto_examples.py tests/test_pluto_zipline_oracle.py tests/test_pluto_isolation.py tests/test_pluto_assets.py`)
- [ ] **M5** hygiene and docs
  - [x] isolation test `tests/test_pluto_isolation.py` (gui, styx, PyQt6, PySide6 blocked in a child interpreter): pass
  - [x] `__main__.py` imports PyQt6/gui only in `backtester`; `install-examples` ran with gui/styx/PyQt6 blocked and no config file
  - [x] `dev/README.md` (the restored path, environment, tests, scope)
  - [ ] fresh-clone run (`git clone --branch restore-sim`, `dev/setup_env.sh` into a new env, build, tests)
  - [ ] `git diff origin/dev --stat` with one line per file

## Evidence

### M1 baseline (as of commit after config fix)

`pytest tests/test_pluto_examples.py`: 5 collected, **5 failed**, all with
`AttributeError: module 'pluto.control.controller.behavior' has no attribute
'SimulationControllerBehavior'` at `pluto/test_utils/test_utils.py:247`.
Cause: the last commit (`816ecbc2` "checkpoint") moved the price lookup from
the data source into the controller behaviour and changed
`Controller(manager, market, tracker, data_source, behavior)` to
`Controller(manager, market, tracker, behavior)`, but the callers
(`test_utils.py`, the test, the GUI) and `SimulationControllerBehavior` were not
updated; `Controller.clock_update` calls `ingest_watchlist_data`,
`ingest_all_data` and `update_watchlist(ts, manager)`, which only
`styx/controller_behavior.py` implements.

### M3 `alpha` root cause (momentum_pipeline 32/39 rows)

- With `empyrical 0.5.5` the fork's **plain Zipline** run of `zipline/examples/momentum_pipeline.py`
  differs from the stored `alpha` by max 1.357 — the same 1.357 Pluto showed. So Pluto == Zipline;
  the stored file disagrees with both (`drivers/zipline_vs_expected.py`-style check, kept outside the repo).
- Recomputing alpha from the run's own returns with the linear annualisation
  `nanmean(returns - beta*benchmark) * 252` reproduces the stored `alpha` of **all 6** zipline
  examples to ≤ 1e-11 (buy_and_hold 7.9e-18, momentum_pipeline 1.9e-14, olmar 8.3e-12).
- empyrical sources: `alpha_aligned` returns `nanmean(alpha_series) * ann_factor` in 0.3.4, 0.4.3,
  0.5.0, 0.5.2 and `(1 + nanmean(...)) ** ann_factor - 1` in 0.5.3 and 0.5.5 (0.5.1 is not on PyPI).
  Zipline's own floor is `empyrical>=0.5.0`.
- With `empyrical==0.5.2`: plain Zipline equals the stored results on every numeric checked column
  for all 6 examples (alpha max diff 0 … 8.3e-12); only `positions` differs, and only by the order of
  the list (same dicts). Pluto: 5/5 pass.
- The other 4 examples passed before only because their alpha is small (max diff 3.5e-5 … 2.8e-4 < atol 0.02).

### M4 Pluto-specific test files (pytest, env36, empyrical 0.5.2)

| File | Status | Reason / area |
|---|---|---|
| `tests/test_pluto_examples.py` | 5/5 pass | in-memory path vs stored results |
| `tests/test_pluto_zipline_oracle.py` (new) | 5/5 pass | in-memory path vs plain Zipline |
| `tests/test_pluto_isolation.py` (new) | 1/1 pass | no gui/styx on the path |
| `tests/test_pluto_assets.py` | 24/24 pass | was 23/24: missing helper `build_lookup_generic_cases` (fixed by importing it from `tests/test_assets.py`) |
| `tests/data/test_clickhouse_minute_bars.py` | collection error | superseded (ClickHouse): `pluto/trading_calendars/compound_calendar.py:7` puts `@abstractmethod` above `@property` (`AttributeError: attribute '__isabstractmethod__' of 'property' objects is not writable`); after that it needs a ClickHouse server. Not touched |
| `tests/data/test_pluto_minute_bars.py` | no tests | empty file (0 bytes) |
| `tests/test_live.py` | collection error | superseded (live): `ModuleNotFoundError: No module named 'ib'` (IbPy2), also uses `alpaca_trade_api` |
| `pluto/coms/utils/test_cert.py` | collection error | superseded (old gRPC/TLS): `ImportError: cannot import name 'CertificateSigningRequestBuilder'` |
| `gui/test.py` | no tests | superseded (GUI scratch file) |

Zipline regression guard (out of scope, run to check the `zipline/` edits): the same 12 files on the
M1 baseline snapshot (`git archive 02720dc9`, built) and on this branch. Nothing got worse:

| File | M1 baseline | this branch |
|---|---|---|
| `tests/test_blotter.py` | 12 errors | 12 errors |
| `tests/finance/test_slippage.py` | 4 failed, 22 errors | same |
| `tests/finance/test_commissions.py` | 1 passed, 7 failed, 9 errors | same |
| `tests/finance/test_cancel_policy.py` | 2 passed | 2 passed |
| `tests/test_tradesimulation.py` | 2 errors | 2 errors |
| `tests/metrics/test_metrics.py` | 7 errors | 7 errors |
| `tests/pipeline/test_engine.py` | 21 passed, 21 failed, 5 errors | same |
| `tests/test_examples.py` | 1 collection error | **4 passed**, 2 failed (`positions` list order only) |
| `tests/test_algorithm.py` | 1 collection error | 110 errors (now collects) |
| `tests/test_ordering.py` | 1 collection error | 1 passed, 26 errors |
| `tests/test_bar_data.py` | 28 errors | 28 errors |
| `tests/test_history.py` | 135 errors | 135 errors |

The remaining errors are the fork's, e.g. `AttributeError: 'int' object has no attribute 'fget'`
in the zipline test fixtures, `'AssetFinder' object has no attribute 'asset_finder'` (the fork's
pipeline engine takes a controllable), `TypeError: __init__() missing ... 'session_id'` (fork's Order).

## Decisions

- **empyrical pinned to 0.5.2** (environment, not data): the stored expected results were produced with
  the linear alpha annualisation; 0.5.2 is the newest release with it and satisfies zipline's
  `empyrical>=0.5.0`. No expected file and no tolerance was changed. Alternative left to the owner:
  keep a newer empyrical and re-baseline the `alpha` column of the expected results.
- **Controller/behaviour**: followed the last commit's design (`816ecbc2`): the behaviour owns the
  price lookup and the data hooks; `Controller(manager, market, tracker, behavior)`. The simulation
  behaviour keeps its own data portals (`add_data_portal`). Rejected: going back to
  `Controller(..., data_source, behavior)` of `3d478ba7` (undoes the author's latest step);
  making the behaviour wrap a `SimulationDataSource` (duplicate abstraction, no caller needs it).
  `pluto/data/data_sources/` stays (used by the GUI, out of scope).
- **zipline run path**: repaired in place (smallest edits, listed in commit `zipline: restore the fork's
  own run path`) rather than using an upstream zipline install, because the owner asked for the fork's
  own path in the same environment.

- **tests/__init__.py**: removed the import instead of restoring `setup`/`teardown`
  in `zipline/__init__.py` (the fork commented that module out entirely; reviving it
  would import all of zipline on `import zipline`). Consequence: zipline's legacy numpy
  print options are not set for nose runs of the zipline doctests (out of scope).
- **configuration**: missing default file → `{}`; an explicit `PLUTO_CONFIG` must exist.
  Alternative rejected: shipping a default toml in the repo (would carry ClickHouse
  settings into a path that does not use them).
- **clickhouse-driver** stays a required package: `pluto/data/bundles/test.py` imports it
  (and ClickHouse writers/readers) at module level. No server is contacted on the
  in-memory path. Making it lazy would mean editing the ClickHouse data layer (out of scope).

## Next step

M5: fresh clone of `restore-sim` into a scratch dir, `dev/setup_env.sh` into a new env, run the
Pluto suite; then the `git diff origin/dev --stat` summary below; then the final report.
