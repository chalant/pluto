# PyCleanup progress: delete what will not be ported (Python 3.13 port, stage 1)

Branch `py313-cleanup`, started at `master` = `b890bb28`. Worktree:
`/home/yves/Documents/coding-projects/pluto-pyclean` (removed at the end; the branch stays).
Not pushed, not merged. Scratch: `/home/yves/.cache/hades-pluto-dev/py313/PyCleanup/`.

Goal (owner decisions 2026-11-21, PM16/PM21 and the strategy-store answer in
`plans/Python Modernisation.md`): on the old Python 3.6 stack, with no behaviour change on the
in-memory simulation path, delete (a) the SQLite strategy store, (b) the launcher leftovers,
(c) off-path modules with SyntaxErrors or undefined names and the named leftovers, and fix
(d) the `.proto` sources (no regeneration). Kept on purpose: the gRPC/protobuf service path
(`protos/`, `pluto/coms`, `pluto/server`, `controllable/server.py`, `pluto/interface/`,
`dev/cli.py`), live/paper code the tests load, `zipline/` (replaced wholesale in the next step).

## How to run

```bash
WT=/home/yves/Documents/coding-projects/pluto-pyclean
PY=/home/yves/.cache/hades-pluto-dev/env36/bin/python
P=/home/yves/.cache/hades-pluto-dev/py313/PyCleanup
(cd $WT && HOME=/home/yves/.cache/hades-pluto-dev/home-restore $PY setup.py build_ext --inplace -j 8)
(cd $WT && HOME=/home/yves/.cache/hades-pluto-dev/home-restore NOSE_PARAMETERIZED_NO_WARN=1 \
  $PY -m pytest -p no:cacheprovider -q tests/test_pluto_examples.py tests/test_pluto_zipline_oracle.py \
  tests/test_pluto_isolation.py tests/test_pluto_assets.py tests/test_pluto_scenarios.py tests/test_pluto_state.py)
# smoke: import, install-examples into a temporary HOME (deleted afterwards)
(cd $WT && HOME=/home/yves/.cache/hades-pluto-dev/home-restore $PY -c "import pluto")
# (the command is the repository-root __main__.py; there is no pluto/__main__.py, so `python -m pluto` does not exist)
T=$(mktemp -d $P/ie.XXXX); (cd $WT && HOME=$T $PY __main__.py install-examples); find $T/.pluto -maxdepth 2; rm -rf $T
# import-resolution scan (copy of the cleanup-superseded scanner, outside the repo)
$PY $P/import_scan.py $WT $P/scan_<step>.json
# protos (scratch Python 3.13 venv, grpcio-tools 1.85.3, protobuf 7.36.2; output outside the repo)
(cd $WT && $P/venv313/bin/python -m grpc_tools.protoc -I. --python_out=$P/protoc_out protos/*.proto)

## Checklist

- [x] worktree + branch at `b890bb28`; extensions built (rc=0, 18 `.so`; `_int64window` 977,344 bytes, not the
  broken 15 KB race result)
- [x] baseline: **61 passed** in 58.75 s; import scan: **64 unresolved pairs**, identical to the
  `cleanup-superseded` after-scan; 1 parse error (`domain_filter.py:109`)
- [x] proto baseline (grpcio-tools 1.85.3): 24 of 27 compile; `hub.proto`, `interface.proto`, `login.proto` fail
- [x] inventory (below), committed before any deletion
- [x] (a) strategy-store database layer and `TestClient` helper (plus `pluto/control/setup/`, `dev/backtester.py`,
  see the note under (a)): **61 passed** (56.87 s); `import pluto` ok; `install-examples` rc=0, rerun rc=0,
  `~/.pluto` has `data/test`, `launchers`, `setup`, `strategies/{5 examples}.py`, `strategies/{states,performance}`
  (no `metadata.sqlite`, no `templates/`); `--help` lists only `install-examples`; scan 64 pairs, new = [],
  gone = []. Helper check: the 5 copies are byte-identical to the sources (`filecmp`), registered in the
  `Directory`, and a second copy raises `FileExistsError`
- [x] (b) launcher leftovers (`script_handler.py`, `example_setup.py`, `launchers` directory): **61 passed**
  (55.85 s); `import pluto` ok; `install-examples` rc=0 and rerun rc=0 (no `launchers/` any more); scan 64,
  new = [], gone = []
- [x] (c) off-path dead modules (29 files, −2,764 lines): **61 passed** (56.94 s); `import pluto` ok;
  `install-examples` rc=0, rerun rc=0; scan **55 pairs**, new = [], gone = 9 (the 7 `pluto/control/modes/*`
  pairs, `alpaca.py` → `pluto.data.bundles:pluto`, `writer.py` → `pluto.universes:schema`); **0 parse errors**
  (the `domain_filter.py` SyntaxError is gone). Directories left with only `__pycache__`
  (`pluto/control/modes`, `pluto/pipeline/filters`, `pluto/data/download/resources`) removed from the
  worktree (untracked; such a directory would import as a namespace package)
- [ ] (d) proto source fixes
- [ ] `dev/README.md`
- [ ] fresh clone + `dev/setup_env.sh` proof; final grep; worktree removed

## Inventory (at `b890bb28`)

"CP" = in `PyScan/closure_union.json` (modules the 61 tests load). Importers from the AST scan
(`import_scan.py --removed <candidate>`: first-party importers outside the candidate) and `git grep`
of the module name in every tracked file (`.vscode/tags`, a stale ctags file of an older `contrib/`
tree, is ignored). "Import in env36" = `python -c "import <module>"`.

### (a) Strategy-store database layer: delete

The owner's decision lists these; four are CP only because the test helper
(`pluto/test_utils/test_utils.py:8,362,366` → `dev/strategy_store.py`) and `__main__.py` load them.
The helper is replaced (no database), so they leave the closure.

| Path | Lines | CP | Importers | Decision |
|---|---:|---|---|---|
| `dev/strategy_store.py` | 102 | yes | `__main__.py:13`, `pluto/test_utils/test_utils.py:8` | delete; callers use the script-file helper |
| `pluto/strategies/strategies.py` | 248 | yes | `dev/strategy_store.py`, `dev/backtester.py`, `pluto/control/setup/setup.py` (all deleted) | delete |
| `pluto/strategies/schema.py` | 52 | yes | `dev/strategy_store.py`, `dev/backtester.py` | delete |
| `pluto/strategies/sessions.py` | 50 | yes | `pluto/strategies/strategies.py` | delete |
| `pluto/strategies/templates.py` | 55 | no | `pluto/pluto_setup.py` (writes `templates/pipeline.py` for `add-strategy`) | delete |
| `pluto/pluto_setup.py` | 42 | no | `__main__.py` only | delete; `install-examples` uses `pluto/setup/directory_setup.py` (the layout the tests use) |
| `directory/loader.py` | 105 | no | `pluto/pluto_setup.py` only | delete (orphaned by the above) |
| `pluto/strategies/setup.py` | 68 | yes | `controllable/server.py:32` (`StrategySetup`) | **keep** `StrategySetup`; drop the SQL texts, `get_strategy_setup`, `save_strategy_setup` (no caller; `get_strategy_setup` calls `execute` without a statement) and the `sqlalchemy` import |
| `pluto/setup/directory_setup.py` | 39 | yes | tests, `server.py`, `engine_setup.py` | keep; drop `strategies/metadata.sqlite` and the unused engine; add the script-file helper |
| `pluto/test_utils/test_utils.py` `TestClient` | — | yes | `tests/test_pluto_examples.py:199-211` | `add_strategy` copies the script with the helper instead of the store; signature unchanged, so no test file changes |
| `__main__.py` `add-strategy` | 26 | no | — | delete (needs the store and the templates) |
| `__main__.py` `add-manager` | 15 | no | — | delete (needs `pluto_setup`; its `launchers/<name>.py` was consumed only by the removed GUI `backtester`) |
| `__main__.py` `install-examples` | — | no | — | keep: `directory_setup.create_directory(~/.pluto)`, copy each example to `strategies/<name>.py` (skip existing), ingest the test bundle |

Moved into (a) during the step (they import the deleted modules; leaving them for (b)/(c) made the
scan grow by 3 pairs between commits): `pluto/control/setup/` (listed under (b); its `StrategyStore`
reads `strategies/metadata.sqlite` through `strategies.get_strategy`) and `dev/backtester.py` (listed
under (c); imports `pluto.strategies.{strategies,schema}`).

Interim convention (documented in `dev/README.md`): a run's script is the file
`<root>/strategies/<strategy_id>.py`, read by `ControllableService.Initialize`
(`controllable/server.py:240`); `<root>/strategies/states` and `<root>/strategies/performance` hold state
and performance files. The helper `directory_setup.add_strategy_script(root_dir, name, source_path)`
copies a script there and raises `FileExistsError` if it exists (the store raised the same error for a
known name; `install-examples` skips it). No metadata database, sessions, status or templates.

Not part of the store, kept and listed for the owner: `pluto/setup/schema.py` (111 lines, SQLAlchemy
tables for exchanges, brokers, accounts; no importer; imports fine), `pluto/setup/account_setup.py` (7,
no importer), `pluto/setup/account.py` (0 bytes).

### (b) Launcher leftovers: delete

| Path | Lines | CP | Importers | Decision |
|---|---:|---|---|---|
| `pluto/control/setup/{__init__,setup,account}.py` | 101 | no | none (only the removed `gui/backtesting.py`) | delete (`StrategyStore` there reads the deleted metadata DB) |
| `pluto/control/script_handler.py` | 46 | no | none (only the removed GUI) | delete |
| `pluto/control/example_setup.py` | 24 | no | none; the launcher script example (`setup(context)`, `context.add_strategy`) that `ScriptHandler` runs | delete |
| `directory_setup.py:28` `launchers` directory | 1 | yes | read only by `add-manager` | delete the line |

### (c) Off-path modules with SyntaxErrors or undefined names, and named leftovers

F821 sites from `PyScan/findings_final.json` (24): `dev/backtester.py` ×8, `docs/notebooks/tutorial.ipynb` ×2,
`pluto/coms/client/ledger.py` ×1, `local_process.py` ×2, `pod_process.py` ×2, `morningstar.py` ×1,
`alpaca.py` ×1, `simulation_broker.py` ×2, `benchmark_metrics.py` ×1, `filters/universes.py` ×2, `graph.py` ×2.

| Path | Lines | Defect | Import in env36 | Importers | Decision |
|---|---:|---|---|---|---|
| `pluto/control/clock/domain_filter.py` | 225 | SyntaxError :109 | SyntaxError | none | delete |
| `dev/backtester.py` | 163 | F821 ×8 | ok | none | delete |
| `pluto/pipeline/filters/universes.py` | 58 | F821 `finder`, `api` (every `compute` raises NameError) | ok | none (only file of `pluto/pipeline/filters/`) | delete |
| `pluto/control/processes/local_process.py` | 57 | F821 `process_factory` | NameError | none | delete |
| `pluto/control/processes/pod_process.py` | 18 | F821 `process_factory` | NameError | none | delete |
| `pluto/data/universes/alpaca.py` | 47 | F821 `alpaca` | ImportError (`pluto.data.bundles.pluto`) | none | delete |
| `pluto/finance/broker/simulation_broker.py` | 57 | F821 `SESSION_START`, `BAR` | ok | none | delete |
| `pluto/finance/metrics/benchmark_metrics.py` | 31 | F821 `kwargs` | ok | none | delete |
| `pluto/utils/graph.py` | 546 | F821 `data`, `node_bytes` | ok | none | delete (`protos/graph.proto`/`graph_pb2.py` stay: `protos/` is not in scope) |
| `pluto/data/download/executors/morningstar.py` | 86 | F821 `symbol_data` | ok | none | delete; rest of `pluto/data/download/` kept (K36, owner) |
| `pluto/coms/client/ledger.py` | 777 | F821 `broker` (:472) | fails (protobuf: `account_state.proto` misses imports) | none | **report**: `pluto/coms` is protected |
| `docs/notebooks/tutorial.ipynb` | — | F821 `figsize` ×2 (`%pylab` name) | — | — | **report**: Zipline documentation notebook, not code |
| `pluto/pipeline/loaders/loader_factory.py` | 59 | `from collections import Iterable` | ImportError (`blaze`) | none (`loaders/__init__.py` is empty) | delete (whole module dead) |
| `pluto/data/universes/writer.py` | 76 | `from collections import Iterable` | ImportError (`pluto.universes`) | none | delete (whole module dead) |
| `pluto/data/download/resources/{__init__,resources,resources_db_schema}.py` | 173 | `declarative_base` import; `Table` misuse | AttributeError | `resources_db_schema` ← `resources.py` ← none | delete the dead cluster (deleting only the schema would leave a new unresolved import) |
| `pluto/interface/directory.py`, `pluto/interface/credentials.py` | — | `declarative_base` | — | — | **report**: `pluto/interface/` is protected |
| `pluto/data/schema.py` | 44 | `Table` without `MetaData` (K34) | AttributeError | none | delete |
| `tests/data/test_pluto_minute_bars.py` | 0 | empty | — | — | delete |
| `pluto/control/modes/` (12 files) | 842 | only user of the old `clock_pb2` fields (`mode.py:102,157,159`); `mode.py` imports the missing `pluto.broker` | ModuleNotFoundError for `mode`, `simulation_mode`, `live_mode`, `live_simulation_mode`, `utils`, `market/factory` | `pluto/control/loop/loop.py:17` only | delete (dead: only dead code uses it) |
| `pluto/control/loop/loop.py` | 438 | imports `modes` | ModuleNotFoundError (`pluto.broker`) | none | delete (`loop/__init__.py`, `simulation_loop.py`, `utils.py` stay) |

### `clock_pb2` drift (PM20)

`protos/clock_pb2.py` is left as it is (regenerated in the next step). Python users of the fields that
exist only in the generated module (`ClockEvent.real_ts`, `ClockEvent.signals`, `Signal.exchange`):
`pluto/control/modes/mode.py:102` (`real_ts=real_dt`), `:157` (`real_ts=...`), `:159` (`signals=signals`);
no other user (`git grep 'real_ts|signals='`; `.exchange` hits elsewhere are assets, clocks in
`domain_filter.py`/`loop.py` (deleted) and `pluto/data/download/downloader.py:40,43` (kept, off path,
reads `clock_evt.exchange`, which neither version of `ClockEvent` has)). Kept users of
`ClockEvent` (`events_log.py:232`, `processes.py:78`) set only `timestamp`/`event`, which both versions
have. Decision: delete `mode.py` with `modes/` (item c), no adaptation needed.

### (d) Proto sources

| File | Error (grpcio-tools 1.85.3) | Fix |
|---|---|---|
| `protos/interface.proto:5` | `Import "data.proto" was not found` (then `"Chunk" is not defined`) | `import "protos/data.proto";` |
| `protos/hub.proto:8,17,18` | `Import "protos/controller.proto" was listed twice`; `"RunParams" is not defined` | drop the duplicate import; import `protos/params.proto` (defines `RunParams`) |
| `protos/login.proto:5` | `Expected ";"` | `;` after the `rpc` declaration |

### Baseline unresolved first-party imports (64 pairs)

Identical to the `cleanup-superseded` after-scan listed in `CLEANUP_PROGRESS.md` (compared as JSON).

## Evidence

(filled in per step)

## Next step

Step (d): `protos/interface.proto:5` → `import "protos/data.proto";`; `protos/hub.proto`: drop the duplicate
`import "protos/controller.proto";` (line 8), add `import "protos/params.proto";`; `protos/login.proto:5`: add
`;`. Compile every file alone and all together with `$P/venv313`; `check.sh WT PY d`; commit.
