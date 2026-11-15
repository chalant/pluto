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
# tests (sandbox HOME: the tests write ~/pluto_tmp and ~/tmp/zipline)
export HOME=~/.cache/hades-pluto-dev/home-restore NOSE_PARAMETERIZED_NO_WARN=1
~/.cache/hades-pluto-dev/env36/bin/python -m pytest -p no:cacheprovider tests/test_pluto_examples.py
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
- [ ] **M2** controller/behaviour refactor completed for simulation, in memory
- [ ] **M3** alignment: 5/5 vs stored results; `momentum_pipeline` alpha root cause; Zipline-vs-Pluto oracle
- [ ] **M4** Pluto test files classified / passing
- [ ] **M5** isolation test (no `gui`/`styx`), lazy GUI import in `__main__.py`, README, fresh-clone run, diff summary

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

## Decisions

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

M2: complete the behaviour interface and wire `SimulationControllerBehavior` into
`pluto/test_utils/test_utils.py` and `tests/test_pluto_examples.py`.
