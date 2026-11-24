# Leftovers closeout (branch `py313-leftovers`)

Base: `origin/master` = `4f9d2519`. Worktree: `/home/yves/.cache/hades-pluto-dev/leftovers/wt`; env
`/home/yves/.cache/hades-pluto-dev/leftovers/env313` (built with `dev/setup_env.sh`); scratch
`/home/yves/.cache/hades-pluto-dev/leftovers/scratch`. Owner decisions PM24 to PM27 and the `test_resource_path`
collection fix, one commit each. A successor continues from the Status list.

## Status

- [x] PM24 drop the pandas 0.22 stored results
- [x] PM25 delete `pluto/_protocol.pyx`
- [x] PM27 fix the zipline.testing pipeline-engine fixtures (owner decision, added during the work)
- [x] PM26 delete dead code, stale files, the 2019 test-suite copy
- [x] `test_resource_path` collected as a test
- [x] final run from a fresh clone (71 passed)

## Removed

### PM24: `example_data/expected_perf/0-22-0/` in `tests/resources/example_data.tar.gz`, and the code that used it

What it was: the expected daily result frames of the five examples (plus `olmar`, which Pluto has no port of) as
written by Zipline's `rebuild_example_data` with pandas 0.22.0 (pickled DataFrames, directory `0-22-0` = pandas
version with dashes; members dated 2018-05-05). First in the archive with upstream Zipline commit `6c590552 2018-07-10
Richard Frank MAINT: Enable example data rebuilding` (that commit adds `0-19-2` and `0-22-0` next to `0-18-1`; the
archive was touched once more upstream, `f4882c63 2018-07-27 Joe Jevnik TST: update example data`, 0-22-0 still there) and used by Pluto's `tests/test_pluto_examples.py` as the
reference of its pandas-0.22 era runs.

Why dropped (owner decision PM24): on the Python 3.13 stack (pandas 2.3.3, numpy 2.5.4, empyrical-reloaded 0.7.3) Pluto
does not reproduce them exactly and cannot, by construction (empyrical-reloaded 0.7.3 computes `alpha` as
`(1 + mean) ** n - 1`, the 0.22-era empyrical as `mean * n`, PM3); `benchmark_volatility` and `beta` differ by about
one unit in the last place in some rows (cause not analysed, numpy/pandas rounding is the likely one, [INFERENCE]).
Pluto equals the vendored zipline bit for bit on all of them (`tests/test_pluto_zipline_oracle.py`), so these are
differences of the stack. Five tests were `expectedFailure` and the rest compared at a tolerance.

What replaces it: the `2-3-3` set (`example_data/expected_perf/2-3-3`, generated on this stack by
`tests/resources/add_expected_perf.py`, which runs zipline's own `run_example` on the example data with the benchmark
returns Pluto reads), compared by `PlutoExamplesTests.test_simulation` in `tests/test_pluto_examples.py`
(`check_new_expected`: `test_utils.frame_errors` with `rtol=0, atol=0`, same index, no extra columns on the Pluto
side). That test already existed and is unchanged: Pluto equals the 2-3-3 set EXACTLY on all five examples (it passes
at `rtol=0, atol=0`; measured by the run, see Final run).

Removed code (`tests/test_pluto_examples.py` first commit `fb0de158 2020-03-19 Yves Chalant examples test` (log tail:
`ccfab963 2020-03-15`), last before this change `f6762e82 2026-11-21 Yves Chalant`; `git log -S`: the key `0-22-0` in
Pluto's test helpers from `2cb82849 2026-11-21`, `STORED_EXPECTED_DIFFERENCES` from `27732ff7 2026-11-21`,
`stored_with_utc_timestamps` from `2cb82849 2026-11-21`):

| What | Where | Notes |
|---|---|---|
| `STORED_PANDAS_VERSION = '0-22-0'` | `tests/test_pluto_examples.py` | lookup key of the 0.22 set |
| `STORED_EXPECTED_DIFFERENCES` (dict, 37 lines with its comment; 43 with `STORED_PANDAS_VERSION` and `NEW_PANDAS_VERSION` above it) | `tests/test_pluto_examples.py` | verbatim below |
| `cls._expected_perf` (dataframe_cache of the 0.22 directory) | `PlutoExamplesTests.setUpClass` | |
| `check_stored`, `test_stored_results` (5 tests), `test_stored_results_expected_differences` (5 expectedFailure tests) | `PlutoExamplesTests` | 0.22 comparison, all columns not in the dict at the tolerance; the dict's columns as expected failures |
| `STORED_RTOL = {'momentum_pipeline': 2e-12}` (+ 8 line comment) | `pluto/test_utils/test_utils.py` | verbatim below |
| `stored_with_utc_timestamps` (22 lines) | `pluto/test_utils/test_utils.py` | pandas 2 loads Timestamps nested in the 0.22 pickles naive; localised them to UTC; no other user (`grep -rn stored_with_utc_timestamps`) |

`STORED_EXPECTED_DIFFERENCES`, verbatim, with its comment (tuples: rows that differ, rows, largest absolute difference,
largest relative difference; measured with pandas 2.3.3, numpy 2.5.4, empyrical-reloaded, at the 0.22 tolerances):

```python
# the stored expected results of the pandas 0.22 era (PM19), compared at the
# tolerances of pluto.test_utils.test_utils (STORED_RTOL), and the set generated
# on this stack by zipline's own run path, stored under the pandas version in use
STORED_PANDAS_VERSION = '0-22-0'
NEW_PANDAS_VERSION = pd.__version__.replace('.', '-')

# Columns in which Pluto differs from the stored 0.22 results on this stack, at
# the existing tolerances (STORED_RTOL, exact otherwise). Measured with
# pandas 2.3.3, numpy 2.5.4, empyrical-reloaded (Python 3.13 port):
# example -> column -> (rows that differ, rows, largest absolute difference,
# largest relative difference). Pluto equals the vendored zipline on all of them
# (tests/test_pluto_zipline_oracle.py), so they are differences of the stack, not of
# Pluto: ``alpha`` is computed by empyrical-reloaded 0.7.3 as
# (1 + mean(alpha series)) ** annualization - 1, where the empyrical of the 0.22
# era multiplied the mean by the annualization factor (PM3);
# ``benchmark_volatility`` and ``beta`` differ by about one unit in the last place in
# some rows (cause not analysed further: numpy/pandas rounding is the likely one). These comparisons are marked
# unittest.expectedFailure; every other column is compared at the existing tolerance.
STORED_EXPECTED_DIFFERENCES = {
    'buy_and_hold': {
        'alpha': (1258, 1259, 3.51572e-05, 4.17854e-03),
        'benchmark_volatility': (669, 1259, 1.11022e-16, 3.90999e-16),
        'beta': (527, 1259, 1.30104e-18, 7.10548e-16),
    },
    'buyapple': {
        'alpha': (210, 211, 2.79377e-04, 1.18417e-02),
        'benchmark_volatility': (17, 211, 2.77556e-17, 2.21463e-16),
        'beta': (106, 211, 2.08167e-17, 2.44270e-15),
    },
    'dual_ema_talib': {
        'alpha': (210, 211, 5.62621e-06, 1.67483e-03),
        'benchmark_volatility': (17, 211, 2.77556e-17, 2.21463e-16),
        'beta': (118, 211, 1.73472e-18, 8.77257e-16),
    },
    'dual_moving_average': {
        'alpha': (202, 502, 2.25119e-07, 3.34869e-04),
        'benchmark_volatility': (87, 502, 2.77556e-17, 3.09713e-16),
        'beta': (89, 502, 5.42101e-19, 1.55319e-15),
    },
    'momentum_pipeline': {
        'alpha': (38, 39, 1.35744, 0.602379),
    },
}
```

`STORED_RTOL` and its comment, verbatim (relative tolerance per example for the comparison with the 0.22 results,
atol 0, every other example exact):

```python
# Relative tolerance per example for the comparison with the stored expected
# results (tests/resources/example_data.tar.gz); atol is 0 and every other
# example is compared exactly. Pluto equals the fork's Zipline bit for bit on
# all five examples (tests/test_pluto_zipline_oracle.py). For
# momentum_pipeline both differ from the stored results by floating-point
# summation noise, measured 2026-11-20 (ALIGN_PROGRESS.md): largest relative
# difference 1.06e-12 (alpha), 6.4e-13 (pnl), largest absolute 1.3e-10
# (ending_cash, portfolio_value ~1e5).
STORED_RTOL = {'momentum_pipeline': 2e-12}
```

Archive rewrite (`tests/resources/example_data.tar.gz`): the tar stream was decompressed, the byte ranges of the
7 members of `example_data/expected_perf/0-22-0` (the directory and 6 files; there is no `._0-22-0` resource-fork
entry) were cut out at 512-byte header boundaries (pax extended header blocks go with their member), everything else
was copied block for block (so every other member is byte-identical, header included), and the stream was gzipped again
(level 9, mtime 0). Checked: names, sizes, types and SHA-256 of the content of all 153 remaining members are equal to
the originals.

| | before (`origin/master`) | after |
|---|---|---|
| members | 160 | 153 |
| `.tar.gz` bytes | 4937417 (sha256 `bbc262d4...35b`) | 3923901 (sha256 `50a115e1...95d`) |
| tar bytes (uncompressed) | 15462400 | 12328960 |

Removed members (size in bytes): `expected_perf/0-22-0/` (dir), `buy_and_hold` 499651, `buyapple` 132002, `dual_ema_talib`
82428, `dual_moving_average` 179050, `momentum_pipeline` 81094, `olmar` 2147500.

The `0-18-1` and `0-19-2` members are NOT deleted (as asked). References to them: none in code or tests
(`grep -rIn '0-18-1\|0-19-2\|0\.18\.1\|0\.19\.2' . --exclude-dir=.git --exclude-dir=docs`: only the legacy
`.travis.yml`/`.appveyor.yml` pandas pins and `Dockerfile`, unrelated). Nothing reads them since the
Python 3.13 port; they are 0.18.1 (6 files, 3.1 MB) and 0.19.2 (6 files, 3.4 MB) pickles. [INFERENCE: removable as dead
weight, left for the owner.]

Member lists (`tar tvf`, mode/size/name) are in the appendix at the end.

### PM25: `pluto/_protocol.pyx` (1002 lines)

What it was: Pluto's own copy of Zipline's Cython module `zipline/_protocol.pyx` as of the 2019 Zipline state (header
`Copyright 2016 Quantopian`, docstring edits of Zipline commits `249124f2 2019-07-22` / `05417dcc 2019-07-23 Scott
Sanderson`): the `check_parameters` decorator class, `handle_non_market_minutes` context manager, `cdef class BarData`
(`current`, `history`, `can_trade`, `is_stale`, the data the strategy sees as `data`), `cdef class SidView`,
`cdef class InnerPosition`. It was meant to become Pluto's own `BarData` (`pluto/finance/blotter/simulation_blotter.py`
docstrings still say `zipline._protocol.BarData`); it differs from the vendored `zipline/_protocol.pyx` by 692 diff lines
(`diff <(git show 4f9d2519:zipline/_protocol.pyx) pluto/_protocol.pyx | wc -l`; mostly the 2019 vs zipline-reloaded 3.2.1
versions, Pluto-specific edits not analysed [INFERENCE]).

Where from: added to Pluto by `48bc9293 2022-07-25 chalant engine refactoring` (`git log --diff-filter=A`; one commit
only in Pluto's history of the path); `git log --follow` traces it to `16fd6681 2016-04-04 Eddie Hebert ENH: Rewrite of
Zipline to use lazy access pattern` (upstream Zipline, 42 commits, last `48bc9293`).

Why dead:

- not in `setup.py` (`ext_modules` has `extension("zipline._protocol")` at line 62 and, from `pluto/`, only
  `extension("pluto.control.clock.sim_engine")`: nothing named `pluto._protocol`);
- no importer: `grep -rn "_protocol" pluto zipline tests dev setup.py` finds only `zipline._protocol` (the vendored module:
  `zipline/protocol.py:22`, `zipline/algorithm.py:30`, `zipline/finance/_finance_ext.pyx:9`, tests) and docstrings
  `zipline._protocol.BarData`; there is no `pluto._protocol` anywhere, no `pluto/_protocol.pxd`, no built `.so`;
- does not compile: `cython -3 -I . pluto/_protocol.pyx` (Cython 3.3.1 of the env) reports
  `pluto/_protocol.pyx:220:69: undeclared name not builtin: data_frequency`,
  `:492:44` and `:497:41: Call with wrong number of arguments (expected 6, got 5)`; and
  `from collections import Iterable` (line 24) and `from six import ... PY2` do not exist on Python 3.13 [INFERENCE for
  these two, not reported before the first errors stop compilation].

Replaces: nothing (the vendored `zipline/_protocol.pyx` is what `zipline.protocol` and Pluto use).

### PM26a: dead Python modules

Method for every item: (1) `grep -rn` of the module path and of its dotted name, and of the names it defines, over
`pluto/ zipline/ tests/ dev/ protos/ setup.py etc/ *.py` (and the whole tree for strings); (2) a static import graph of
all `*.py` files (`ast`, absolute and relative imports, scratch script `scratch/graph.py`) with the roots
`tests/test_pluto_*.py`, `tests/pluto_scenarios/`, `tests/resources/add_expected_perf.py`: 91 of the 169 `pluto.*`
modules are reachable, the others are listed by the graph with their importers; (3) `python -c "import <module>"` in
the env of this port; (4) `setup.py` (`ext_modules` are `.pyx` only; no `packages` data, no entry points) and the dev
scripts do not name them. `PYCLEAN_PROGRESS.md` had protected `pluto/coms`; the owner's PM26 decision lifts it for the
items below.

#### `pluto/coms/client/` (4 files, 937 lines) and `pluto/utils/saving.py` (36 lines)

| path | lines |
|---|---|
| `pluto/coms/client/ledger.py` | 777 |
| `pluto/coms/client/protos/account_state_pb2.py` | 160 |
| `pluto/coms/client/__init__.py`, `pluto/coms/client/protos/__init__.py` | 0 each |
| `pluto/utils/saving.py` (consequence, see below) | 36 |

What it was: `ledger.py` (2018-11-29 "grpc messaging first commit" to 2022-05-10 "checkpoint", all by Yves Chalant /
chalant; `git log --follow` first `d9298f09 2018-11-29 Yves Chalant grpc messaging first commit`, last `ba1e6da5
2022-05-10 chalant checkpoint`) is the client-side account of the old gRPC architecture: `BrokerClient` (lines 35-123:
converts zipline orders to `protos.broker_pb2` messages and broker state back, `order`, `cancel`, `update`,
`get_transactions`) and `Ledger(saving.Savable)` (lines 124-777: the strategy's virtual account on top of the broker:
`process_order`, `orders`, `update(...)` from the broker's state, `_process_splits`, `_process_dividends`,
`_earn_dividends`, `_pay_dividends`, portfolio/positions/account updates, `get_state` / `_restore_state` through the
`account_state_pb2.AccountState` message). `account_state_pb2.py` is the generated module of `AccountState` and
`Return` (source `contrib/coms/client/protos/account_state.proto`, first committed 2019-03-14 in `1b1703e2`). `saving.py` is the
`Savable` ABC (`get_state(dt)`, `restore_state(state)`, `_restore_state`) whose only user was `ledger.py`.

Why dead: no importer anywhere (`grep -rn "coms.client\|coms/client"`: only the files themselves and
`pluto/finance/metrics/tracker_state_pb2.py`, itself dead, below); `import pluto.coms.client.ledger` and
`import pluto.coms.client.protos.account_state_pb2` fail with `TypeError: Couldn't build proto file into descriptor pool:
Depends on file 'contrib/coms/protos/protocol.proto', but it has not been loaded` (no `contrib/` `.proto` exists in the
repository; `git ls-files | grep -E "account_state\.proto|contrib/"` is empty); `ledger.py` also has `drs.append(pd.Series(...))`
(line 256, pandas 1 API; `Series.append` is gone in pandas 2), `pd.Series()` without dtype (line 268) and the undefined
name `broker` (line 472, F821, recorded in `PYCLEAN_PROGRESS.md`). `saving.py`: the only importer was `ledger.py`
(`from pluto.utils import saving`; the graph lists no other), so deleting `ledger.py` orphans it; deleted with it (not on
the owner's list: consequence, delete-with-its-only-user).

Replaces: `pluto/finance/ledger.py` (the live `Ledger`, used by `pluto/finance/metrics/tracker.py` and tested by
`tests/test_pluto_state.py`, `tests/test_pluto_upstream_fixes.py`) and `protos/ledger_state_pb2.py` /
`protos/tracker_state_pb2.py` for the saved state (regenerated from `.proto` files that exist).

#### `pluto/data/io/` (3 files, 247 lines) and `pluto/data/bundler.py` (53 lines)

| path | lines | what |
|---|---|---|
| `pluto/data/io/__init__.py` | 23 | `asset_finder(file_path)` (`pluto.assets.assets.AssetFinder`), `adjustments_reader` (`zipline.data.adjustments.SQLiteAdjustmentReader`), `daily_bars_reader` (`zipline.data.hdf5_daily_bars.HDF5DailyBarReader`), `minute_bars_reader` (raises `NotImplementedError`), `unsupported_error` |
| `pluto/data/io/daily_bars.py` | 206 | `DailyBarWriter(file_path, date_chunk_size).write(country_code, frames)`: writes the HDF5 daily bar file (groups `data`, `index`, `lifetimes`, scaling factors of `zipline.data.hdf5_daily_bars`) |
| `pluto/data/io/paths.py` | 18 | `bundle(name)`, `daily_bars`, `minute_bars`, `assets`, `adjustments`: file paths below `pluto.interface.utils.paths.get_dir('data')` |
| `pluto/data/bundler.py` (consequence) | 53 | `Bundle` value class and `get_bundle_loader(platform)`: for platform `'pluto'` a `load(bundle_name)` that reads the four files through `pluto.data.io` |

Origin: `io/*` first commit `9d8cfb4d 2020-01-03 Yves Chalant checkpoint`, last `4fed72c6 2020-02-26 Yves Chalant checkpoint`
(`__init__`); `bundler.py` first `9d8cfb4d 2020-01-03`, last `526a1ff5 2022-06-14 chalant backtesting ui`.

Why dead: no importer except `pluto/data/bundler.py` (`from pluto.data.io import paths`, `from pluto.data import io`) and
nothing imports `pluto.data.bundler` (graph: "imported by []"; `grep -rnI "bundler"` outside history files: only itself).
Importing any of them outside a `directory` context fails at import time: `RuntimeError: Outside of directory context`
(`pluto/data/io/daily_bars.py:22` and `paths.py:3` call `paths.get_dir('data')` at module level). The live bundle path is
`pluto/data/bundles/core.py`, `pluto/data/bundles/test.py` (`TestBundle`), `pluto/data/universes/core.py`
(`get_universe`), used by all tests. `bundler.py` could not stay without `io` (it would be an unimportable module), so it
goes with it.

Replaces: `zipline.data.hdf5_daily_bars.HDF5DailyBarWriter` / `HDF5DailyBarReader` (vendored, `zipline/data/hdf5_daily_bars.py`)
for the HDF5 files; `pluto/data/bundles` and `pluto/data/universes` for loading.

#### stale generated protobuf modules (5 files, 1253 lines)

| path | lines | content | import in the env |
|---|---|---|---|
| `pluto/coms/data_bundle_pb2.py` | 287 | `AssetMetadata`, `BundleMetadata`, `Bundle`, `DomainDef`, `UnitDomainDef`, `CompoundDomainDef` (source `data_bundle.proto`, protoc of 2019) | fails: protobuf rejects the old generated code ("More information: https://developers.google.com/protocol-buffers/docs/news/2022-05-06#python-updates") |
| `pluto/coms/data_bundle_pb2_grpc.py` | 3 | `import grpc` only (no service) | imports; stub, no importer (the one item here that imports; its siblings do not and the `.proto` has no service) |
| `pluto/coms/hub_pb2.py` | 716 | 14 messages of `hub.proto` | fails: `ModuleNotFoundError: No module named 'data_bundle_pb2'` (absolute import of a top-level module) |
| `pluto/coms/hub_pb2_grpc.py` | 166 | `HubStub`, `HubServicer`, `add_HubServicer_to_server` | fails: same |
| `pluto/finance/metrics/tracker_state_pb2.py` | 81 | `TrackerState` (source `contrib/finance/metrics/tracker_state.proto`) | fails: `TypeError ... Depends on file 'contrib/coms/protos/protocol.proto'` (it imports the dead `account_state_pb2`) |

Origin: all by Yves Chalant; `pluto/coms/*`: first `5a3e11d1 2019-07-09`, last `99ea06aa 2019-12-20 checkpoint`;
`tracker_state_pb2.py`: first `0e678809 2019-03-24 checkpoint commit new clock api`, last `99ea06aa 2019-12-20`.

Why dead: no importer (`grep` for `pluto.coms.hub`, `pluto.coms.data_bundle`, `metrics.tracker_state_pb2`: none; the
live code imports the top-level `protos` package: `from protos import tracker_state_pb2 as trs` in
`pluto/finance/metrics/tracker.py`, `from protos import ... data_bundle_pb2 as dtb` in `pluto/coms/services/broker_service.py`
and `pluto/coms/controller.py`). The `.proto` sources of `hub` and `data_bundle` exist (`protos/hub.proto`,
`protos/data_bundle.proto`) but not at the paths these modules were generated for, and they were regenerated in `protos/`
during the port (`PY313_PROGRESS.md`: all 25 `protos/*_pb2.py` and 9 `_pb2_grpc.py` import on their own; checked again here
for `protos.hub_pb2`, `hub_pb2_grpc`, `data_bundle_pb2`, `data_bundle_pb2_grpc`, `tracker_state_pb2`, `protocol_pb2`,
`ledger_state_pb2`, `broker_pb2_grpc`). `tracker_state.proto` of `contrib/finance/metrics/` does not exist.

Replaces: `protos/hub_pb2.py`, `protos/hub_pb2_grpc.py`, `protos/data_bundle_pb2.py`, `protos/data_bundle_pb2_grpc.py`,
`protos/tracker_state_pb2.py` (regenerated, importable).

### PM26b: legacy CI, packaging and dev files (Python 2.7 to 3.6 era)

Evidence common to all: the repository's supported setup is `dev/setup_env.sh` (uv, Python 3.13) and
`etc/requirements-py313.txt`; none of these files is referenced by `setup.py`, `MANIFEST.in`, `dev/`, the code or the
kept tests (`grep -rnIF` of each name outside the files of the group and outside the history `*_PROGRESS.md` files and
`.vscode/tags`).

| path | lines | what it did | origin (newest / oldest commit) | why dead | replaced by |
|---|---|---|---|---|---|
| `.travis.yml` | 110 | Travis CI matrix: Python 2.7 (3.5 commented), miniconda 4.3.30, `conda build conda/*` recipes, upload to the `quantopian` anaconda channel; reads `etc/requirements_talib.txt` | `00a8f96b 2019-11-14 Christian Clauss` / `8ead20b6 2012-10-19 Eddie Hebert` | Travis service of Quantopian's repo; Python 2.7/3.5, conda 4.3, a quantopian channel token; `etc/requirements_talib.txt` no longer exists (PM13) | none (no CI in this repo; the test command is in `dev/README.md`) |
| `.appveyor.yml` | 112 | AppVeyor Windows build: Python 2.7 / 3.5, pandas 0.18.1 to 0.22.0, numpy 1.11.3 to 1.14.1, conda build, `ci/appveyor/*` | `6d61194e 2019-11-13 James Meickle` / `96046fc1 2016-03-23 Joe Jevnik` | same; names the removed `etc/requirements_blaze.txt` and `_talib.txt` | none |
| `ci/` (6 files: `make_conda_packages.py` 69, `travis/install_miniconda.sh` 12, `travis/overwrite_requirements.sh` 22, `appveyor/install.ps1` 97, `appveyor/run_with_env.cmd` 95, `appveyor/vcvars64.bat` 1) | 296 | helper scripts of the two CI files: miniconda install, `sed` of `etc/requirements_blaze.txt` (removed), conda package builds with upload, Windows compiler environment | newest `bca90dec 2018-07-11 Richard Frank` / oldest `cbc1316a 2016-03-23 Richard Frank` | only called by `.travis.yml` / `.appveyor.yml`; `overwrite_requirements.sh` edits a requirements file that is gone | none |
| `Dockerfile-dev` | 34 | image `FROM quantopian/zipline`, `pip install -r etc/requirements_dev.txt -r etc/requirements_blaze.txt`, `pip install -e .[all]` | `98481a11 2017-05-23 cap` / `7e70f8fa 2016-06-07 ChrisPappalardo` | base image `quantopian/zipline` (Python 3.5, below); `requirements_blaze.txt` gone; `[all]` extra gone | none |
| `Dockerfile` (not on the owner's list; same category) | 93 | the `quantopian/zipline` image: `FROM python:3.5`, TA-Lib 0.4.0 from source, `pip install 'numpy<2' 'scipy<1.0' 'pandas>=0.18.1,<1.0.0'`, Jupyter notebook server (`etc/docker_cmd.sh`), `pip install -e .` | `0c8fe8b3 2019-11-18 Willian Paixao` / `34f47da0 2016-04-04 John Stanford` | Python 3.5 and pandas < 1.0 pins cannot install this tree; only consumer of `etc/docker_cmd.sh` and `.dockerignore`; parent image of `Dockerfile-dev` | none |
| `.dockerignore` | 6 | ignore list of the Dockerfile build (`MANIFEST.in`, `**/*pyc`, `.eggs`, `dist`, `build`, `*.egg-info`) | `34f47da0 2016-04-04 John Stanford` (single commit) | only meaningful for the deleted `Dockerfile` | none |
| `etc/docker_cmd.sh` | 19 | `CMD` of the Dockerfile: generates a Jupyter config, certificate and password hash, starts the notebook server | `937558ee 2017-07-02 Nathan Graham` / `34f47da0 2016-04-04 John Stanford` | only `Dockerfile` references it (`ADD ./etc/docker_cmd.sh /`) | none |
| `vagrant_init.sh` | 53 | provisioning of the Vagrant VM: `ppa:fkrull/deadsnakes-python2.7`, `apt-get install python2.7 ...`, get-pip, `etc/ordered_pip.sh etc/requirements.txt`, `requirements_dev/blaze` | `aa76fe8d 2016-11-14 Richard Frank` / `d9b7578e 2013-07-02 John Ricklefs` | installs Python 2.7 on `ubuntu/trusty64`; names the removed `requirements_blaze.txt` | `dev/setup_env.sh` |
| `Vagrantfile` (not on the list; same category) | 10 | Vagrant config, box `ubuntu/trusty64`, provisions with `vagrant_init.sh` | `a728ef2b 2016-07-07 Richard Frank` / `d9b7578e 2013-07-02 John Ricklefs` | its only content is the call of the deleted `vagrant_init.sh`; Ubuntu 14.04 box | `dev/setup_env.sh` |
| `etc/ordered_pip.sh` (not on the list; same category) | 12 | installs the lines of a requirements file one by one, in order (`pip install --exists-action w`) | `622144fa 2015-07-23 Jonathan Kamens` / `cca65808 2012-02-07 fawce` | only caller: `vagrant_init.sh`; ordered installs are replaced by `uv pip sync` of the exact pins | `dev/setup_env.sh` |
| `etc/dev-install` | 55 | "installation script for Travis and humans": `pip==19.2.2`, `pip install numpy Cython -c etc/requirements.txt`, `--no-binary=bcolz`, `etc/requirements_dev.txt`, `etc/requirements_blaze.txt` (gone), `pip install -e .[all]` (gone extra) | `463d7fed 2019-11-08 Frederick Wagner` / `b13f0e70 2019-08-26 Frederick Wagner` | pins pip 19.2.2 (no Python 3.13 support), installs removed requirement files and extras; callers: Travis (deleted) and `docs/source/development-guidelines.rst` (upstream text, see doubtful list) | `dev/setup_env.sh` |
| `etc/conda_build_matrix.py` (not on the list; same category) | 118 | click script: `conda build conda/zipline` for Python 2.7/3.4/3.5 x numpy 1.9/1.10, optional upload to the `quantopian` channel | `1e69190e 2017-06-02 Miguel Sanchez de Leon Peque` / `88e47586 2016-02-24 Richard Frank` | Python 2.7 to 3.5 matrix and a quantopian account; no caller (only `.vscode/tags`) | none |
| `etc/git-hooks/pre-commit` (not on the list; same category) | 33 | git hook: `flake8 zipline tests` then `nosetests -x` | `63063b1e 2013-04-01 Eddie Hebert` / `f054835c 2013-03-19 Eddie Hebert` | `nosetests` does not run on Python 3.13 (nose is not installed; `nose_parameterized` imports failed in 30 old test modules, below); the `tests/` it tested is gone; never installed by a script | none |
| `setup.cfg` `[nosetests]` section (edit, 10 lines; the file stays with `[metadata]` and `[easy-install]`) | 10 | nose options: `with-doctest`, `with-timer`, `cover-package=zipline`, `testmatch=...` | `setup.cfg` history of Zipline | read only by nose, which is not installed and cannot run here; the tests run with pytest | pytest (`dev/README.md`) |

### PM26c: the 2019 copy of Zipline's test suite in `tests/`

What it was: upstream Zipline's own `tests/` as of **Zipline master 2019-11-27** (newest upstream-authored commit touching
`tests/`: `6255b714 2019-11-27 Scott Sanderson [skip ci] DOC: Add comments to test.`, `git describe`:
`1.1.0-1082-g6255b714`; the history of this repository holds the upstream tags up to `v1.1.0.4`/`1.1.0`, none for 1.3 or
1.4 [INFERENCE: this is the code of the 1.4.0 development line; 1.4.0 itself was released later]), with Pluto's
"checkpoint" edits on top (last: `bb641160 2020-01-29 Yves Chalant checkpoint`, touching e.g. `tests/test_live.py`;
`tests/test_algorithm.py` last `5fa665c2 2019-08-23`). The `tests/finance/__init__.py`, `tests/metrics/__init__.py`,
`tests/test_live.py`, `tests/pipeline/test_blaze.py`, `tests/resources/rebuild_example_data` etc. are not in
zipline-reloaded 3.2.1 (column "in zr 3.2.1" of the scratch table; `tests/test_live.py` is from the live-trading
fork code merged into the history: first commit `f0c0b6e2 2017-05-15 Tibor Kiss RealtimeClock` [INFERENCE: zipline-live]).

State before deletion, measured in the environment of this port (`pytest tests --ignore-glob='tests/test_pluto_*'
--ignore=tests/pluto_scenarios -p no:randomly`, vendored zipline, scratch `scratch/run_old_v.txt`):
**60 of the test modules fail at collection** (the 12 most frequent causes, 54 of the 60: 30: `ModuleNotFoundError: nose_parameterized`; 4: `cannot import name 'Int64Index' from
'pandas'`; 3: `zipline.data.minute_bars`; 3: `pandas.util.testing`; 3: `blaze`; 2: `trading_calendars`; 2: `nose`; 2: `assert_raises_str`;
2: `assert_is` (not in `zipline.testing.predicates`); 1 each: `zipline.utils.metautils`, `zipline.utils.argcheck`, `ib`);
of the 163 tests that do collect: **128 passed, 29 failed, 1 skipped, 5 setup errors** (the passes are in
`tests/pipeline/test_{classifier,domain,dtypes,filter,pipeline}.py`, `tests/test_{labelarray,memoize,testing,tradesimulation}.py`,
`tests/utils/test_{cache,math_utils,numpy_utils,pandas_utils,sentinel,sharedoc}.py`, `tests/finance/test_{cancel_policy,transaction}.py`,
`tests/data/{test_hdf5_daily_bars,bundles/test_quandl}.py`: 19+11+9+24+11+17+2+8+1+3+1+3+3+10+1+2+1+1+1 = 128). Zipline's own current tests live in
zipline-reloaded (`tests/`, 3.2.1), where the same areas run on this stack (`PY313_PROGRESS.md` C1b: 130 passed, 1 skipped
for three of its files; PM27 below: the pipeline files).

Import graph (static, plus the data files opened at runtime): the seven `tests/test_pluto_*.py` files import, from
`tests`, only `tests.pluto_scenarios`, `tests.test_pluto_examples` and `tests.test_pluto_zipline_oracle`; nothing
imports `tests.utils`, `tests.pipeline.base` or any other deleted module; at run time the only file opened from
`tests/resources/` is `example_data.tar.gz` (through `zipline.testing.core.test_resource_path`, also used by
`pluto/data/bundles/test.py` and by `tests/resources/add_expected_perf.py`); `grep` of `csvdir_samples`, `quandl_samples`,
`yahoo_samples`, `pipeline_inputs`, `fetcher_inputs`, `fetcher_test_data`, `rebuild_example_data`, `daily_bar_writer`,
`generate_csvs` over `pluto zipline dev` and the kept tests finds only code comments
(`pluto/examples/*.py: # rebuild_example_data#L105`, URL comments) and the deleted tests/scripts themselves. Proof by the
final run (below).

**Keep-set of `tests/` (33 files):** `tests/__init__.py`; `tests/test_pluto_{assets,examples,isolation,scenarios,state,upstream_fixes,zipline_oracle}.py` (7);
`tests/pluto_scenarios/` (23 files); `tests/resources/example_data.tar.gz`; `tests/resources/add_expected_perf.py`.
`tests/resources/__init__.py` (empty; nothing imports it) was deleted with the rest.

**Deleted: 115 files**, tables per directory (line counts are `wc -l` of the file at `origin/master`; `def test_`
counts are static, descriptions come from module headers/imports and the names of the tested modules [INFERENCE where not
read in detail]; all from the same Zipline snapshot as above):

#### Top-level test modules (`tests/test_*.py`)

24 files, 378 `def test_` (static count: parameterized, subtests and inherited cases are not counted).

| file | lines | `def test_` | covered |
|---|---|---|---|
| `tests/test_algorithm.py` | 4396 | 78 | TradingAlgorithm: order/order_target*/batch_market_order, schedule_function, record, set_* methods and their validation, pipeline attach, fetcher, trading controls, benchmarks, handle_data/before_trading_start |
| `tests/test_api_shim.py` | 564 | 10 | the deprecated `data[sid]` / `data.history` BarData shim (`ZiplineDeprecationWarning` paths) |
| `tests/test_assets.py` | 2540 | 57 | AssetFinder and the assets db: Equity/Future/ContinuousFuture, lookup_symbol, retrieve_all, schema migrations, exchange info (Pluto kept its own adapted copy: tests/test_pluto_assets.py) |
| `tests/test_bar_data.py` | 1248 | 26 | BarData.current / can_trade / is_stale for equities and futures, minute and daily |
| `tests/test_benchmark.py` | 232 | 4 | BenchmarkSource (benchmark returns, missing data, calendar handling) |
| `tests/test_blotter.py` | 412 | 9 | SimulationBlotter: order, cancel, process_splits, slippage/commission wiring |
| `tests/test_clock.py` | 180 | 5 | the simulation clock (zipline.gens.sim_engine: MinuteSimulationClock, SESSION_START/BAR/SESSION_END sequence) |
| `tests/test_cmdline.py` | 138 | 3 | the `zipline` command line (parse_args, run, ingest options) |
| `tests/test_continuous_futures.py` | 1835 | 29 | ContinuousFuture roll rules, chains, history and adjustments |
| `tests/test_data_portal.py` | 589 | 11 | DataPortal: get_spot_value, history windows, adjustments, futures |
| `tests/test_examples.py` | 88 | 1 | zipline.examples run against the stored expected_perf of example_data.tar.gz (the oracle/example tests of Pluto replace it: test_pluto_examples.py, test_pluto_zipline_oracle.py) |
| `tests/test_execution_styles.py` | 253 | 5 | MarketOrder / LimitOrder / StopOrder / StopLimitOrder price rounding and validation |
| `tests/test_fetcher.py` | 621 | 13 | fetch_csv (needs `responses`; data in tests/resources/fetcher_inputs) |
| `tests/test_finance.py` | 455 | 6 | simulation of trades through SimulationBlotter and MetricsTracker (cash, positions, commissions) |
| `tests/test_history.py` | 2180 | 29 | data.history(): windows, fields, frequencies, adjustments, daily and minute |
| `tests/test_labelarray.py` | 609 | 18 | LabelArray (string/categorical arrays used by pipeline classifiers) |
| `tests/test_live.py` | 1401 | 31 | live trading: IB / Alpaca brokers, RealtimeClock, LiveTradingAlgorithm (imports `ib.ext`, `alpaca_trade_api`, `mock`; Pluto/zipline-live code, not in zipline-reloaded) |
| `tests/test_memoize.py` | 96 | 2 | zipline.utils.memoize.remember_last |
| `tests/test_ordering.py` | 352 | 9 | order handling: order, order_value, order_percent, targets, cancel, limits |
| `tests/test_registration_manager.py` | 119 | 3 | zipline.extensions.Registry (register/unregister of extension objects with an interface) |
| `tests/test_restrictions.py` | 419 | 6 | asset restrictions: StaticRestrictions, HistoricalRestrictions, SecurityListRestrictions, NoRestrictions |
| `tests/test_security_list.py` | 282 | 11 | security lists (leveraged ETF list) with data by date |
| `tests/test_testing.py` | 202 | 10 | zipline.testing helpers (assert helpers, tmp dirs, TestingSlippage, parameter space) |
| `tests/test_tradesimulation.py` | 126 | 2 | AlgorithmSimulator loop with BEFORE_TRADING_START_BAR (zipline.gens.tradesimulation) |

#### `tests/data/` (data bundles, bar readers and writers, adjustments)

11 files, 91 `def test_` (static count: parameterized, subtests and inherited cases are not counted).

| file | lines | `def test_` | covered |
|---|---|---|---|
| `tests/data/__init__.py` | 0 | 0 | package marker (empty) |
| `tests/data/bundles/__init__.py` | 0 | 0 | package marker (empty) |
| `tests/data/bundles/test_core.py` | 558 | 9 | bundle registry and ingest/clean/load (zipline.data.bundles.core) |
| `tests/data/bundles/test_csvdir.py` | 131 | 1 | the csvdir bundle (data in tests/resources/csvdir_samples) |
| `tests/data/bundles/test_quandl.py` | 233 | 1 | the quandl bundle (data in tests/resources/quandl_samples) |
| `tests/data/test_adjustments.py` | 327 | 5 | SQLiteAdjustmentWriter / Reader |
| `tests/data/test_daily_bars.py` | 772 | 21 | BcolzDailyBarWriter / Reader |
| `tests/data/test_dispatch_bar_reader.py` | 332 | 3 | AssetDispatchSessionBarReader / AssetDispatchMinuteBarReader |
| `tests/data/test_hdf5_daily_bars.py` | 75 | 2 | HDF5DailyBarWriter / Reader |
| `tests/data/test_minute_bars.py` | 1249 | 27 | BcolzMinuteBarWriter / Reader |
| `tests/data/test_resample.py` | 894 | 22 | minute-to-daily resampling readers |

#### `tests/events/` (event rules)

4 files, 37 `def test_` (static count: parameterized, subtests and inherited cases are not counted).

| file | lines | `def test_` | covered |
|---|---|---|---|
| `tests/events/__init__.py` | 0 | 0 | package marker (empty) |
| `tests/events/test_events.py` | 504 | 33 | zipline.finance.events rules (date/time rules, EventManager, composition) |
| `tests/events/test_events_cme.py` | 43 | 1 | event rules on the CME calendar |
| `tests/events/test_events_nyse.py` | 196 | 3 | event rules on the NYSE calendar |

#### `tests/finance/` (commissions, slippage, cancel policy, risk, transactions)

6 files, 52 `def test_` (static count: parameterized, subtests and inherited cases are not counted).

| file | lines | `def test_` | covered |
|---|---|---|---|
| `tests/finance/__init__.py` | 0 | 0 | package marker (empty) |
| `tests/finance/test_cancel_policy.py` | 34 | 2 | EODCancel / NeverCancel |
| `tests/finance/test_commissions.py` | 538 | 15 | commission models (PerShare, PerTrade, PerDollar, PerContract, PerFutureTrade) |
| `tests/finance/test_risk.py` | 348 | 17 | risk metrics of the period (volatility, sharpe, sortino, beta, max drawdown) from zipline.finance.risk |
| `tests/finance/test_slippage.py` | 1347 | 17 | slippage models (VolumeShare, FixedSlippage, FixedBasisPoints, futures) |
| `tests/finance/test_transaction.py` | 38 | 1 | Transaction.to_dict |

#### `tests/metrics/` (metrics API)

3 files, 10 `def test_` (static count: parameterized, subtests and inherited cases are not counted).

| file | lines | `def test_` | covered |
|---|---|---|---|
| `tests/metrics/__init__.py` | 0 | 0 | package marker (empty) |
| `tests/metrics/test_core.py` | 105 | 3 | zipline.finance.metrics.core (register, load, unregister) |
| `tests/metrics/test_metrics.py` | 2340 | 7 | the metrics tracker/ledger end to end (returns, cash flow, positions, dividends, splits, benchmark metrics) |

#### `tests/pipeline/` (Pipeline API)

29 files, 374 `def test_` (static count: parameterized, subtests and inherited cases are not counted).

| file | lines | `def test_` | covered |
|---|---|---|---|
| `tests/pipeline/__init__.py` | 0 | 0 | package marker (empty) |
| `tests/pipeline/base.py` | 187 | 0 | helpers: `with_defaults`, `BaseUSEquityPipelineTestCase` |
| `tests/pipeline/test_adjusted_array.py` | 920 | 11 | AdjustedArray and windows (float64, int64, uint8, label) |
| `tests/pipeline/test_adjustment.py` | 102 | 5 | Adjustment objects (Float64Multiply, Add, Overwrite...) |
| `tests/pipeline/test_alias.py` | 65 | 3 | Term.alias |
| `tests/pipeline/test_blaze.py` | 2474 | 45 | blaze Pipeline loaders (blaze removed from the environment: `ModuleNotFoundError: blaze`) |
| `tests/pipeline/test_classifier.py` | 743 | 20 | Classifier terms (eq, startswith, relabel, quantiles) |
| `tests/pipeline/test_column.py` | 119 | 3 | BoundColumn / Column and the `latest` term |
| `tests/pipeline/test_dataset.py` | 105 | 5 | DataSet and column definitions |
| `tests/pipeline/test_domain.py` | 611 | 16 | pipeline domains (US_EQUITIES, GENERIC, country domains, EquitySessionDomain) |
| `tests/pipeline/test_downsampling.py` | 818 | 12 | downsampling of terms (weekly, monthly, quarterly, yearly) |
| `tests/pipeline/test_dtypes.py` | 103 | 0 | dtype rules of terms |
| `tests/pipeline/test_engine.py` | 1677 | 31 | SimplePipelineEngine: run_pipeline, run_chunked_pipeline, workspaces, masks, initial workspace |
| `tests/pipeline/test_events.py` | 604 | 6 | event-based pipeline loaders (EventsLoader, BoundColumn for event datasets) |
| `tests/pipeline/test_factor.py` | 1506 | 37 | Factor terms: rank, zscore, demean, winsor, top/bottom, quantiles, CustomFactor windows |
| `tests/pipeline/test_filter.py` | 1131 | 24 | Filter terms and combinators, All/Any/AtLeastN, SingleAsset, StaticAssets |
| `tests/pipeline/test_frameload.py` | 249 | 3 | DataFrameLoader |
| `tests/pipeline/test_hooks.py` | 533 | 8 | pipeline engine hooks (ProgressHooks, delegating hooks) |
| `tests/pipeline/test_international_markets.py` | 429 | 3 | pipelines on several country domains |
| `tests/pipeline/test_multidimensional_dataset.py` | 355 | 8 | MultiDimensionalDataSet (slicing by extra dimensions) |
| `tests/pipeline/test_numerical_expression.py` | 557 | 16 | NumericalExpression (arithmetic on factors) |
| `tests/pipeline/test_pipeline.py` | 279 | 12 | Pipeline object: add, remove, screen, domain, to_execution_plan |
| `tests/pipeline/test_pipeline_algo.py` | 790 | 10 | pipelines inside TradingAlgorithm (attach_pipeline, pipeline_output; data in tests/resources/pipeline_inputs) |
| `tests/pipeline/test_quarters_estimates.py` | 2709 | 12 | quarterly estimates loaders (earnings-estimates style datasets) |
| `tests/pipeline/test_slice.py` | 528 | 11 | Slice terms (factor[asset]) |
| `tests/pipeline/test_statistical.py` | 1061 | 20 | regression / correlation built-ins (RollingPearson, RollingLinearRegression, Beta) |
| `tests/pipeline/test_technical.py` | 665 | 20 | technical factors (EWMA, RSI, Bollinger, MACD, VWAP, TrueRange, AnnualizedVolatility...) |
| `tests/pipeline/test_term.py` | 821 | 28 | Term, ComputableTerm, dependency graph (TermGraph, ExecutionPlan) |
| `tests/pipeline/test_us_equity_pricing_loader.py` | 658 | 5 | USEquityPricingLoader (bcolz bars + adjustments) |

#### `tests/risk/` and `tests/history/`

3 files, 0 `def test_` (static count: parameterized, subtests and inherited cases are not counted).

| file | lines | `def test_` | covered |
|---|---|---|---|
| `tests/history/generate_csvs.py` | 169 | 0 | script generating history test csv files (generate_daily_test_data / generate_minute_test_data); writes a csv set that no kept code reads |
| `tests/risk/test_risk_cumulative.py` | 0 | 0 | empty file (0 bytes): emptied upstream when the metrics API replaced the risk tracker |
| `tests/risk/test_risk_period.py` | 0 | 0 | empty file (0 bytes): same |

#### `tests/utils/` (zipline.utils)

13 files, 88 `def test_` (static count: parameterized, subtests and inherited cases are not counted).

| file | lines | `def test_` | covered |
|---|---|---|---|
| `tests/utils/__init__.py` | 0 | 0 | package marker (empty) |
| `tests/utils/daily_bar_writer.py` | 50 | 0 | helper `DailyBarWriterFromDataFrames(BcolzDailyBarWriter)` |
| `tests/utils/test_argcheck.py` | 268 | 21 | zipline.utils.argcheck (`ModuleNotFoundError: zipline.utils.argcheck`: module no longer exists) |
| `tests/utils/test_cache.py` | 67 | 3 | zipline.utils.cache (CachedObject, ExpiringCache, dataframe_cache) |
| `tests/utils/test_date_utils.py` | 86 | 2 | zipline.utils.date_utils (compute_date_range_chunks, make_utc_aware) |
| `tests/utils/test_final.py` | 264 | 16 | zipline.utils.final (FinalMeta, final) |
| `tests/utils/test_math_utils.py` | 13 | 1 | zipline.utils.math_utils.tolerant_equals |
| `tests/utils/test_metautils.py` | 91 | 5 | zipline.utils.metautils (`ModuleNotFoundError`: module no longer exists) |
| `tests/utils/test_numpy_utils.py` | 94 | 3 | zipline.utils.numpy_utils (repeat_first_axis, rolling_window...) |
| `tests/utils/test_pandas_utils.py` | 193 | 5 | zipline.utils.pandas_utils (nearest_unequal_elements, timedelta_to_integral...) |
| `tests/utils/test_preprocess.py` | 484 | 21 | zipline.utils.preprocess (preprocess, call, expect_types, expect_bounded) |
| `tests/utils/test_sentinel.py` | 61 | 10 | zipline.utils.sentinel |
| `tests/utils/test_sharedoc.py` | 21 | 1 | zipline.utils.sharedoc |

#### `tests/resources/` (fixture data and rebuild scripts; the kept ones are `example_data.tar.gz` and `add_expected_perf.py`)

22 files, 0 `def test_` (static count: parameterized, subtests and inherited cases are not counted).

| file | lines | `def test_` | covered |
|---|---|---|---|
| `tests/resources/__init__.py` | 0 | 0 | package marker (empty) |
| `tests/resources/csvdir_samples/csvdir/daily/AAPL.csv.gz` | binary |  | daily csv sample of a csvdir bundle (read by tests/data/bundles/test_csvdir.py) |
| `tests/resources/csvdir_samples/csvdir/daily/IBM.csv.gz` | binary |  | daily csv sample of a csvdir bundle (read by tests/data/bundles/test_csvdir.py) |
| `tests/resources/csvdir_samples/csvdir/daily/KO.csv.gz` | binary |  | daily csv sample of a csvdir bundle (read by tests/data/bundles/test_csvdir.py) |
| `tests/resources/csvdir_samples/csvdir/daily/MSFT.csv.gz` | binary |  | daily csv sample of a csvdir bundle (read by tests/data/bundles/test_csvdir.py) |
| `tests/resources/fetcher_inputs/__init__.py` | 0 | 0 | package marker (empty) |
| `tests/resources/fetcher_inputs/fetcher_test_data.py` | 689 | 0 | csv/json strings for test_fetcher.py |
| `tests/resources/pipeline_inputs/AAPL.csv` | 128 |  | price csv read by tests/pipeline/test_pipeline_algo.py |
| `tests/resources/pipeline_inputs/BRK-A.csv` | 128 |  | price csv read by tests/pipeline/test_pipeline_algo.py |
| `tests/resources/pipeline_inputs/MSFT.csv` | 128 |  | price csv read by tests/pipeline/test_pipeline_algo.py |
| `tests/resources/pipeline_inputs/generate.py` | 42 | 0 | script that generated the three csv files below (from yahoo data) |
| `tests/resources/quandl_samples/QUANDL_ARCHIVE.zip` | binary |  | sample data of the quandl bundle test (tests/data/bundles/test_quandl.py) |
| `tests/resources/quandl_samples/metadata.csv.gz` | binary |  | sample data of the quandl bundle test (tests/data/bundles/test_quandl.py) |
| `tests/resources/quandl_samples/rebuild_samples.py` | 78 | 0 | script that rebuilt QUANDL_ARCHIVE.zip / metadata.csv.gz (needs a Quandl API key) |
| `tests/resources/rebuild_example_data` | 196 | 0 | click script that rebuilt tests/resources/example_data.tar.gz (ingests a yahoo bundle, runs the examples, writes expected_perf/<pandas version>); broken: line 35 calls `yahoo_equities(...)`, which is neither imported nor defined anywhere in the repo, so it fails with NameError at start. Replaced for the expected results by tests/resources/add_expected_perf.py; the `root/` data of the tarball cannot be rebuilt any more |
| `tests/resources/yahoo_samples/AAPL.adjustments.gz` | binary |  | yahoo bundle sample (prices / adjustments) of the removed yahoo bundle tests; no reader left in the repository |
| `tests/resources/yahoo_samples/AAPL.csv.gz` | binary |  | yahoo bundle sample (prices / adjustments) of the removed yahoo bundle tests; no reader left in the repository |
| `tests/resources/yahoo_samples/IBM.adjustments.gz` | binary |  | yahoo bundle sample (prices / adjustments) of the removed yahoo bundle tests; no reader left in the repository |
| `tests/resources/yahoo_samples/IBM.csv.gz` | binary |  | yahoo bundle sample (prices / adjustments) of the removed yahoo bundle tests; no reader left in the repository |
| `tests/resources/yahoo_samples/MSFT.adjustments.gz` | binary |  | yahoo bundle sample (prices / adjustments) of the removed yahoo bundle tests; no reader left in the repository |
| `tests/resources/yahoo_samples/MSFT.csv.gz` | binary |  | yahoo bundle sample (prices / adjustments) of the removed yahoo bundle tests; no reader left in the repository |
| `tests/resources/yahoo_samples/rebuild_samples` | 81 | 0 | script that rebuilt the yahoo_samples gz files (downloads from yahoo) |


## Fixes

### `test_resource_path` collected as a test

`zipline/testing/core.py:1282` (1285 after PM27) defines `def test_resource_path(*path_parts)` (path of `tests/resources/<parts>`; the
`@nottest` decorator is commented out in the vendored file). `tests/test_pluto_examples.py` and
`tests/test_pluto_zipline_oracle.py` imported it by name (`from zipline.testing import test_resource_path, core`), so
pytest collected the imported function in those two modules as a test item that "passed" with no arguments (baseline
master: 2 such items [INFERENCE from counts: 83 baseline - 10 dropped 0.22 tests - 71 now collected = 2; the assignment said 3; `grep` finds the import in two
test modules only]). Fix, in Pluto's test files only (vendored zipline untouched): import the module only
(`from zipline.testing import core`) and call `core.test_resource_path(...)`; a module attribute is not collected.
`pytest --collect-only` of the seven files afterwards: 71 items, none named `test_resource_path`.

### PM27: `zipline.testing` pipeline-engine fixtures fixed (owner decision: fix, do not delete)

Problem: Pluto's `SimplePipelineEngine(get_loader, controllable, default_domain=GENERIC, populate_initial_workspace=None,
default_hooks=None)` (K30) takes a controllable (anything with an `.asset_finder` attribute; `engine.py:487` and `:767` read
`self._controllable.asset_finder`), but three helpers in `zipline/testing` still built it the upstream way:

- `zipline/testing/fixtures.py` `WithUSEquityPricingPipelineEngine.init_class_fixtures` (line ~1782):
  `SimplePipelineEngine(get_loader=..., asset_finder=cls.asset_finder, default_domain=US_EQUITIES)` -> `TypeError:
  unexpected keyword argument 'asset_finder'`;
- `fixtures.py` `WithSeededRandomPipelineEngine.init_class_fixtures` (line ~1840): same keyword -> same error;
- `zipline/testing/core.py` `temp_pipeline_engine` (line ~1031): `SimplePipelineEngine(get_loader, calendar, finder)`: the
  positional layout of an older upstream signature (and wrong for upstream 3.2.1 too, where the second argument is
  `asset_finder`: `calendar` would be taken as the controllable and `finder` as `default_domain`, rejected by
  `expect_types`). The assignment called it `make_seeded_random_pipeline_engine`; the function is `temp_pipeline_engine`.
  No caller in this repository (only re-exported by `zipline/testing/__init__.py`).

Change (engine signature untouched): each passes `SimpleNamespace(asset_finder=<finder>)` as the controllable (`from types
import SimpleNamespace` in both modules; no helper, three call sites); the two files carry "Modified by Pluto" header
lines and the `temp_pipeline_engine` docstring is corrected. Other `SimplePipelineEngine(` callers in `zipline/` and
`pluto/`: `zipline/algorithm.py:412` and `pluto/algorithm.py:177` pass the algorithm / `self._controllable`: fine.

Proof, upstream tests of zipline-reloaded (clone `/home/yves/.cache/hades-pluto-dev/py313/port/zr`, tag 3.2.1) run
against this tree (`PYTHONPATH=<worktree>`, cwd `zr`, `-o addopts="" -p no:randomly --maxfail=100000`) on
`tests/pipeline/{test_column,test_domain,test_downsampling,test_engine,test_factor,test_filter,test_hooks,test_slice,test_statistical}.py`
(the nine files that use `pipeline_engine` / `seeded_random_engine`; logs `leftovers/pm27-before.log`, `pm27-after.log`):

| | passed | failed | errors | skipped | xfailed |
|---|---|---|---|---|---|
| before | 488 | 31 | 60 (all `TypeError ... unexpected keyword argument 'asset_finder'` in fixture setup) | 2 | 1 |
| after | 539 | 31 | 8 | 3 | 1 |

52 of the 60 errors are gone through the fixtures (51 more passes, 1 more skip); the other 8 were never the fixtures'
(7 + 1, below). What remains is upstream test code that builds the engine with the upstream API or reads its private
names:

- 31 failed = 30 with `AttributeError: 'AssetFinder' object has no attribute 'asset_finder'` at `engine.py:487` plus 1
  `SubTestFailures` (`PopulateInitialWorkspaceTestCase`, same AttributeError inside); in `test_engine.py` (21:
  `ConstantInputTestCase` 14, `ParameterizedFactorTestCase` 3, `SyntheticBcolzTestCase` 2, `FrameInputTestCase` 1,
  `PopulateInitialWorkspaceTestCase` 1) and `test_statistical.py::TestStatisticalBuiltIns` (10): those tests call
  `SimplePipelineEngine(loader, asset_finder, ...)` themselves (17 such calls in the upstream `tests/`);
- 7 errors `test_column.py::LatestTestCase`: `AttributeError: 'SimplePipelineEngine' object has no attribute '_finder'`
  (`test_column.py:48`, `cls.engine._finder`; Pluto's engine stores `_controllable`);
- 1 error `test_downsampling.py::TestDownsampledRowwiseOperation`: the test builds `SimplePipelineEngine(get_loader=...,
  asset_finder=cls.asset_finder, ...)` itself (line 760).

Also run after (`test_us_equity_pricing_loader.py`, `test_pipeline_algo.py`, `test_bundle_pipeline.py`, `test_testing.py`):
42 passed, 2 failed (`test_testing.py::TestTestingSlippage::test_fill_all` and `test_constant_filled_per_tick`:
`TypeError: Order.__init__() missing 1 required positional argument: 'amount'`, unrelated to the engine, not fixed;
not run before the change [INFERENCE: not caused by it]).

## Final run

Fresh clone `git clone --no-local --branch py313-leftovers <worktree> .../leftovers/verify` at `153afba7`, fresh environment from
`dev/setup_env.sh` (34 s, uv cache warm), then, from the clone:

```
python -c 'import pluto, zipline'     -> import ok
python -m pytest -p no:randomly tests/test_pluto_examples.py tests/test_pluto_zipline_oracle.py tests/test_pluto_isolation.py \
    tests/test_pluto_assets.py tests/test_pluto_scenarios.py tests/test_pluto_state.py tests/test_pluto_upstream_fixes.py
-> 71 passed in 61.09 s, 0 failed, 0 xfailed, 0 skipped
python -m pytest --collect-only -q tests | grep -c test_resource_path   -> 0
```

Delta from the baseline of master (78 passed + 5 xfailed = 83): minus 5 passed (`test_stored_results`, one per example, the 0.22
comparison at the existing tolerance), minus 5 xfailed (`test_stored_results_expected_differences`), minus 2 passed that were the
imported function `test_resource_path` collected as a test (83 - 10 - 2 = 71; the assignment expected 3 of those, only two modules import
it). The five `test_simulation` tests compare Pluto with the `2-3-3` set at `rtol=0, atol=0` and pass: Pluto equals it exactly.

PM27 was verified separately on the upstream zipline-reloaded tests (table in its section above).

## Left in place (doubtful), not deleted

Reported, not deleted (not clearly dead, or outside what the decisions name):

- `tests/resources/example_data.tar.gz` members `expected_perf/0-18-1/` and `expected_perf/0-19-2/` (6 files each, 3.1 MB and
  3.4 MB): no reference in code or tests (PM24 asked to keep them).
- `conda/` (25 files: conda recipes of `zipline` and of its 2019 dependencies; newest commit `a55ee7b9 2020-01-03 Yves Chalant`,
  oldest `e4caa431 2014-02-16 twiecki`): packaging of the removed Python 2.7 to 3.6 setup, called only by the deleted CI files
  (`conda build conda/$recipe`) and by the deleted `etc/conda_build_matrix.py`, but the owner touched it in 2020: not deleted.
- `docs/` (Sphinx sources of Zipline 1.x with notebooks and `docs/deploy.py`): `docs/source/development-guidelines.rst`
  (lines 30, 38, 47: `etc/dev-install`; 77: the Dockerfile; 139: AppVeyor) and `docs/source/release-process.rst`
  (205, 218, 228: AppVeyor, `etc/conda_build_matrix.py`) now point to deleted files.
- `etc/requirements_dev.txt` (kept: updated for 3.13 by the port; its only callers were the deleted scripts), `etc/requirements_docs.txt`
  (Sphinx pins from 2016, used by `docs/`), `etc/goodies.txt` (2012 pins `matplotlib==1.5.3 scipy==0.12.0 ...`, referenced
  nowhere), `etc/rebuild-cython.sh` (deletes `.c`/`.so` in `zipline tests` and runs `setup.py build_ext --inplace`, still
  works), `etc/create_authors_file.sh`, `etc/gen_type_stubs.py` (release process), `.coveragerc` (omits
  `*/site-packages/nose/*`), `.dir-locals.el`, `.github/ISSUE_TEMPLATE.md` (the Zipline issue template), `MANIFEST.in`.
- `.vscode/tags` (ctags index; lists the deleted paths) and `.vscode/settings.json`.
- Root files: `Testing.ipynb` (15867 bytes), `Untitled6.ipynb` (2609), `benchmark_series`, `pluto_returns`, `pluto_daily_returns` (CSV / text
  data, 45 to 62 KB, read by no code: `grep` finds only `_calculate_benchmark_series` in `pluto/sources/benchmark_source.py`),
  `structs.py`, `__main__.py`, and the history files `*_PROGRESS.md`.
- `zipline/testing/` was not touched except PM27.
- Pluto modules not reachable by the static import graph from the tests, after this change (64 of 155 `pluto.*` modules;
  "unreachable from the tests" is NOT "dead": the gRPC services, the interface/CLI, `pluto.examples.*` (loaded by file path) and
  similar are entry points that no test imports). No importer-free proof was made for them, none deleted:
  `pluto.algorithm_setup pluto.assets.exchange pluto.client pluto.client.cli pluto.client.client pluto.coms pluto.coms.clock_client pluto.coms.clock_server pluto.coms.controllable pluto.coms.controller pluto.coms.services pluto.coms.services.broker_service pluto.coms.utils.assertion pluto.coms.utils.certification pluto.coms.utils.kube_certification pluto.coms.utils.server_utils pluto.coms.utils.test_cert pluto.control pluto.control.commands pluto.control.controllable.controllable_states pluto.control.controllable.models pluto.control.controllable.models.commission pluto.control.controllable.recovery pluto.control.controllable.run_states pluto.control.controllable.storage_path pluto.control.loop.utils pluto.control.metadata pluto.data.calendars pluto.data.data pluto.data.data_portal pluto.data.data_sources.live_data_source pluto.data.streaming pluto.examples.buy_and_hold pluto.examples.buyapple pluto.examples.dual_ema_talib pluto.examples.dual_moving_average pluto.examples.momentum_pipeline pluto.explorer pluto.explorer.explorer pluto.finance.blotter.controllable_blotter pluto.finance.blotter.controller_blotter pluto.finance.broker.broker pluto.finance.broker.broker_service pluto.finance.commission pluto.finance.commission.models pluto.finance.commission.schema pluto.finance.slippage pluto.finance.slippage.setup pluto.finance.tax pluto.gens pluto.interface.cli pluto.interface.credentials pluto.interface.directory pluto.interface.hub pluto.interface.interface pluto.interface.manager pluto.interface.utils.grpc_interceptors pluto.interface.utils.security pluto.managers pluto.pipeline.loaders pluto.resources.template pluto.server.service pluto.utils.files pluto.utils.paths`.

## Appendix: members of `tests/resources/example_data.tar.gz`

### before (160, `origin/master`)

```
drwxrwxr-x 0 example_data/
-rw-r--r-- 6148 example_data/.DS_Store
-rw-r--r-- 120 example_data/._.DS_Store
-rwxr-xr-x 222 example_data/._expected_perf
-rwxr-xr-x 222 example_data/._root
drwxrwxr-x 0 example_data/expected_perf/
-rw-r--r-- 6148 example_data/expected_perf/.DS_Store
-rw-r--r-- 120 example_data/expected_perf/._.DS_Store
-rwxr-xr-x 222 example_data/expected_perf/._0-18-1
drwxrwxr-x 0 example_data/expected_perf/0-18-1/
-rw-r--r-- 499733 example_data/expected_perf/0-18-1/buy_and_hold
-rw-r--r-- 132123 example_data/expected_perf/0-18-1/buyapple
-rw-r--r-- 82525 example_data/expected_perf/0-18-1/dual_ema_talib
-rw-r--r-- 179144 example_data/expected_perf/0-18-1/dual_moving_average
-rw-r--r-- 81062 example_data/expected_perf/0-18-1/momentum_pipeline
-rw-r--r-- 2147507 example_data/expected_perf/0-18-1/olmar
drwxrwxr-x 0 example_data/expected_perf/0-19-2/
-rw-r--r-- 612578 example_data/expected_perf/0-19-2/buy_and_hold
-rw-r--r-- 151525 example_data/expected_perf/0-19-2/buyapple
-rw-r--r-- 101293 example_data/expected_perf/0-19-2/dual_ema_talib
-rw-r--r-- 208110 example_data/expected_perf/0-19-2/dual_moving_average
-rw-r--r-- 86322 example_data/expected_perf/0-19-2/momentum_pipeline
-rw-r--r-- 2289637 example_data/expected_perf/0-19-2/olmar
drwxrwxr-x 0 example_data/expected_perf/0-22-0/
-rw-r--r-- 499651 example_data/expected_perf/0-22-0/buy_and_hold
-rw-r--r-- 132002 example_data/expected_perf/0-22-0/buyapple
-rw-r--r-- 82428 example_data/expected_perf/0-22-0/dual_ema_talib
-rw-r--r-- 179050 example_data/expected_perf/0-22-0/dual_moving_average
-rw-r--r-- 81094 example_data/expected_perf/0-22-0/momentum_pipeline
-rw-r--r-- 2147500 example_data/expected_perf/0-22-0/olmar
drwxrwxr-x 0 example_data/expected_perf/2-3-3/
-rw-rw-r-- 629873 example_data/expected_perf/2-3-3/buy_and_hold
-rw-rw-r-- 155346 example_data/expected_perf/2-3-3/buyapple
-rw-rw-r-- 102510 example_data/expected_perf/2-3-3/dual_ema_talib
-rw-rw-r-- 209781 example_data/expected_perf/2-3-3/dual_moving_average
-rw-rw-r-- 89381 example_data/expected_perf/2-3-3/momentum_pipeline
-rw-rw-r-- 2361338 example_data/expected_perf/2-3-3/olmar
drwxrwxr-x 0 example_data/root/
-rwxr-xr-x 222 example_data/root/._data
-rw-r--r-- 222 example_data/root/._extension.py
drwxrwxr-x 0 example_data/root/data/
-rwxr-xr-x 222 example_data/root/data/._test
-rw-r--r-- 301968 example_data/root/data/SPY_benchmark.csv
drwxrwxr-x 0 example_data/root/data/test/
-rwxr-xr-x 222 example_data/root/data/test/._2016-12-13T19;59;34.153449
drwxrwxr-x 0 example_data/root/data/test/2016-12-13T19;59;34.153449/
-rw-r--r-- 222 example_data/root/data/test/2016-12-13T19;59;34.153449/._adjustments.sqlite
-rw-r--r-- 222 example_data/root/data/test/2016-12-13T19;59;34.153449/._assets-6.sqlite
-rwxr-xr-x 222 example_data/root/data/test/2016-12-13T19;59;34.153449/._daily_equities.bcolz
-rwxr-xr-x 222 example_data/root/data/test/2016-12-13T19;59;34.153449/._minute_equities.bcolz
-rw-r--r-- 106496 example_data/root/data/test/2016-12-13T19;59;34.153449/adjustments.sqlite
-rw-r--r-- 90112 example_data/root/data/test/2016-12-13T19;59;34.153449/assets-7.sqlite
drwxrwxr-x 0 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/
-rw-r--r-- 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/.___attrs__
-rw-r--r-- 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/.___rootdirs__
-rwxr-xr-x 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/._close
-rwxr-xr-x 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/._day
-rwxr-xr-x 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/._high
-rwxr-xr-x 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/._id
-rwxr-xr-x 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/._low
-rwxr-xr-x 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/._open
-rwxr-xr-x 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/._volume
-rw-r--r-- 493 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/__attrs__
-rw-r--r-- 67 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/__rootdirs__
drwxrwxr-x 0 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/close/
-rw-r--r-- 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/close/.___attrs__
-rwxr-xr-x 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/close/._data
-rwxr-xr-x 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/close/._meta
-rw-r--r-- 3 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/close/__attrs__
drwxrwxr-x 0 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/close/data/
-rw-r--r-- 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/close/data/.___0.blp
-rw-r--r-- 50182 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/close/data/__0.blp
drwxrwxr-x 0 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/close/meta/
-rw-r--r-- 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/close/meta/._sizes
-rw-r--r-- 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/close/meta/._storage
-rw-r--r-- 54 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/close/meta/sizes
-rw-r--r-- 115 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/close/meta/storage
drwxrwxr-x 0 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/day/
-rw-r--r-- 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/day/.___attrs__
-rwxr-xr-x 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/day/._data
-rwxr-xr-x 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/day/._meta
-rw-r--r-- 3 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/day/__attrs__
drwxrwxr-x 0 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/day/data/
-rw-r--r-- 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/day/data/.___0.blp
-rw-r--r-- 9961 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/day/data/__0.blp
drwxrwxr-x 0 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/day/meta/
-rw-r--r-- 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/day/meta/._sizes
-rw-r--r-- 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/day/meta/._storage
-rw-r--r-- 54 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/day/meta/sizes
-rw-r--r-- 115 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/day/meta/storage
drwxrwxr-x 0 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/high/
-rw-r--r-- 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/high/.___attrs__
-rwxr-xr-x 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/high/._data
-rwxr-xr-x 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/high/._meta
-rw-r--r-- 3 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/high/__attrs__
drwxrwxr-x 0 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/high/data/
-rw-r--r-- 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/high/data/.___0.blp
-rw-r--r-- 50172 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/high/data/__0.blp
drwxrwxr-x 0 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/high/meta/
-rw-r--r-- 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/high/meta/._sizes
-rw-r--r-- 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/high/meta/._storage
-rw-r--r-- 54 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/high/meta/sizes
-rw-r--r-- 115 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/high/meta/storage
drwxrwxr-x 0 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/id/
-rw-r--r-- 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/id/.___attrs__
-rwxr-xr-x 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/id/._data
-rwxr-xr-x 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/id/._meta
-rw-r--r-- 3 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/id/__attrs__
drwxrwxr-x 0 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/id/data/
-rw-r--r-- 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/id/data/.___0.blp
-rw-r--r-- 549 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/id/data/__0.blp
drwxrwxr-x 0 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/id/meta/
-rw-r--r-- 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/id/meta/._sizes
-rw-r--r-- 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/id/meta/._storage
-rw-r--r-- 54 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/id/meta/sizes
-rw-r--r-- 115 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/id/meta/storage
drwxrwxr-x 0 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/low/
-rw-r--r-- 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/low/.___attrs__
-rwxr-xr-x 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/low/._data
-rwxr-xr-x 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/low/._meta
-rw-r--r-- 3 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/low/__attrs__
drwxrwxr-x 0 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/low/data/
-rw-r--r-- 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/low/data/.___0.blp
-rw-r--r-- 50187 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/low/data/__0.blp
drwxrwxr-x 0 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/low/meta/
-rw-r--r-- 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/low/meta/._sizes
-rw-r--r-- 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/low/meta/._storage
-rw-r--r-- 54 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/low/meta/sizes
-rw-r--r-- 115 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/low/meta/storage
drwxrwxr-x 0 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/open/
-rw-r--r-- 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/open/.___attrs__
-rwxr-xr-x 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/open/._data
-rwxr-xr-x 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/open/._meta
-rw-r--r-- 3 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/open/__attrs__
drwxrwxr-x 0 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/open/data/
-rw-r--r-- 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/open/data/.___0.blp
-rw-r--r-- 50208 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/open/data/__0.blp
drwxrwxr-x 0 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/open/meta/
-rw-r--r-- 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/open/meta/._sizes
-rw-r--r-- 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/open/meta/._storage
-rw-r--r-- 54 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/open/meta/sizes
-rw-r--r-- 115 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/open/meta/storage
drwxrwxr-x 0 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/volume/
-rw-r--r-- 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/volume/.___attrs__
-rwxr-xr-x 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/volume/._data
-rwxr-xr-x 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/volume/._meta
-rw-r--r-- 3 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/volume/__attrs__
drwxrwxr-x 0 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/volume/data/
-rw-r--r-- 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/volume/data/.___0.blp
-rw-r--r-- 82397 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/volume/data/__0.blp
drwxrwxr-x 0 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/volume/meta/
-rw-r--r-- 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/volume/meta/._sizes
-rw-r--r-- 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/volume/meta/._storage
-rw-r--r-- 54 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/volume/meta/sizes
-rw-r--r-- 115 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/volume/meta/storage
drwxrwxr-x 0 example_data/root/data/test/2016-12-13T19;59;34.153449/minute_equities.bcolz/
-rw-r--r-- 222 example_data/root/data/test/2016-12-13T19;59;34.153449/minute_equities.bcolz/._metadata.json
-rw-r--r-- 141144 example_data/root/data/test/2016-12-13T19;59;34.153449/minute_equities.bcolz/metadata.json
-rw-r--r-- 965021 example_data/root/data/treasury_curves.csv
-rw-r--r-- 0 example_data/root/extension.py
```

### after (153)

```
drwxrwxr-x 0 example_data/
-rw-r--r-- 6148 example_data/.DS_Store
-rw-r--r-- 120 example_data/._.DS_Store
-rwxr-xr-x 222 example_data/._expected_perf
-rwxr-xr-x 222 example_data/._root
drwxrwxr-x 0 example_data/expected_perf/
-rw-r--r-- 6148 example_data/expected_perf/.DS_Store
-rw-r--r-- 120 example_data/expected_perf/._.DS_Store
-rwxr-xr-x 222 example_data/expected_perf/._0-18-1
drwxrwxr-x 0 example_data/expected_perf/0-18-1/
-rw-r--r-- 499733 example_data/expected_perf/0-18-1/buy_and_hold
-rw-r--r-- 132123 example_data/expected_perf/0-18-1/buyapple
-rw-r--r-- 82525 example_data/expected_perf/0-18-1/dual_ema_talib
-rw-r--r-- 179144 example_data/expected_perf/0-18-1/dual_moving_average
-rw-r--r-- 81062 example_data/expected_perf/0-18-1/momentum_pipeline
-rw-r--r-- 2147507 example_data/expected_perf/0-18-1/olmar
drwxrwxr-x 0 example_data/expected_perf/0-19-2/
-rw-r--r-- 612578 example_data/expected_perf/0-19-2/buy_and_hold
-rw-r--r-- 151525 example_data/expected_perf/0-19-2/buyapple
-rw-r--r-- 101293 example_data/expected_perf/0-19-2/dual_ema_talib
-rw-r--r-- 208110 example_data/expected_perf/0-19-2/dual_moving_average
-rw-r--r-- 86322 example_data/expected_perf/0-19-2/momentum_pipeline
-rw-r--r-- 2289637 example_data/expected_perf/0-19-2/olmar
drwxrwxr-x 0 example_data/expected_perf/2-3-3/
-rw-rw-r-- 629873 example_data/expected_perf/2-3-3/buy_and_hold
-rw-rw-r-- 155346 example_data/expected_perf/2-3-3/buyapple
-rw-rw-r-- 102510 example_data/expected_perf/2-3-3/dual_ema_talib
-rw-rw-r-- 209781 example_data/expected_perf/2-3-3/dual_moving_average
-rw-rw-r-- 89381 example_data/expected_perf/2-3-3/momentum_pipeline
-rw-rw-r-- 2361338 example_data/expected_perf/2-3-3/olmar
drwxrwxr-x 0 example_data/root/
-rwxr-xr-x 222 example_data/root/._data
-rw-r--r-- 222 example_data/root/._extension.py
drwxrwxr-x 0 example_data/root/data/
-rwxr-xr-x 222 example_data/root/data/._test
-rw-r--r-- 301968 example_data/root/data/SPY_benchmark.csv
drwxrwxr-x 0 example_data/root/data/test/
-rwxr-xr-x 222 example_data/root/data/test/._2016-12-13T19;59;34.153449
drwxrwxr-x 0 example_data/root/data/test/2016-12-13T19;59;34.153449/
-rw-r--r-- 222 example_data/root/data/test/2016-12-13T19;59;34.153449/._adjustments.sqlite
-rw-r--r-- 222 example_data/root/data/test/2016-12-13T19;59;34.153449/._assets-6.sqlite
-rwxr-xr-x 222 example_data/root/data/test/2016-12-13T19;59;34.153449/._daily_equities.bcolz
-rwxr-xr-x 222 example_data/root/data/test/2016-12-13T19;59;34.153449/._minute_equities.bcolz
-rw-r--r-- 106496 example_data/root/data/test/2016-12-13T19;59;34.153449/adjustments.sqlite
-rw-r--r-- 90112 example_data/root/data/test/2016-12-13T19;59;34.153449/assets-7.sqlite
drwxrwxr-x 0 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/
-rw-r--r-- 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/.___attrs__
-rw-r--r-- 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/.___rootdirs__
-rwxr-xr-x 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/._close
-rwxr-xr-x 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/._day
-rwxr-xr-x 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/._high
-rwxr-xr-x 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/._id
-rwxr-xr-x 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/._low
-rwxr-xr-x 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/._open
-rwxr-xr-x 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/._volume
-rw-r--r-- 493 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/__attrs__
-rw-r--r-- 67 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/__rootdirs__
drwxrwxr-x 0 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/close/
-rw-r--r-- 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/close/.___attrs__
-rwxr-xr-x 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/close/._data
-rwxr-xr-x 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/close/._meta
-rw-r--r-- 3 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/close/__attrs__
drwxrwxr-x 0 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/close/data/
-rw-r--r-- 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/close/data/.___0.blp
-rw-r--r-- 50182 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/close/data/__0.blp
drwxrwxr-x 0 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/close/meta/
-rw-r--r-- 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/close/meta/._sizes
-rw-r--r-- 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/close/meta/._storage
-rw-r--r-- 54 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/close/meta/sizes
-rw-r--r-- 115 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/close/meta/storage
drwxrwxr-x 0 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/day/
-rw-r--r-- 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/day/.___attrs__
-rwxr-xr-x 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/day/._data
-rwxr-xr-x 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/day/._meta
-rw-r--r-- 3 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/day/__attrs__
drwxrwxr-x 0 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/day/data/
-rw-r--r-- 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/day/data/.___0.blp
-rw-r--r-- 9961 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/day/data/__0.blp
drwxrwxr-x 0 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/day/meta/
-rw-r--r-- 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/day/meta/._sizes
-rw-r--r-- 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/day/meta/._storage
-rw-r--r-- 54 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/day/meta/sizes
-rw-r--r-- 115 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/day/meta/storage
drwxrwxr-x 0 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/high/
-rw-r--r-- 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/high/.___attrs__
-rwxr-xr-x 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/high/._data
-rwxr-xr-x 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/high/._meta
-rw-r--r-- 3 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/high/__attrs__
drwxrwxr-x 0 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/high/data/
-rw-r--r-- 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/high/data/.___0.blp
-rw-r--r-- 50172 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/high/data/__0.blp
drwxrwxr-x 0 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/high/meta/
-rw-r--r-- 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/high/meta/._sizes
-rw-r--r-- 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/high/meta/._storage
-rw-r--r-- 54 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/high/meta/sizes
-rw-r--r-- 115 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/high/meta/storage
drwxrwxr-x 0 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/id/
-rw-r--r-- 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/id/.___attrs__
-rwxr-xr-x 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/id/._data
-rwxr-xr-x 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/id/._meta
-rw-r--r-- 3 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/id/__attrs__
drwxrwxr-x 0 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/id/data/
-rw-r--r-- 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/id/data/.___0.blp
-rw-r--r-- 549 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/id/data/__0.blp
drwxrwxr-x 0 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/id/meta/
-rw-r--r-- 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/id/meta/._sizes
-rw-r--r-- 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/id/meta/._storage
-rw-r--r-- 54 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/id/meta/sizes
-rw-r--r-- 115 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/id/meta/storage
drwxrwxr-x 0 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/low/
-rw-r--r-- 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/low/.___attrs__
-rwxr-xr-x 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/low/._data
-rwxr-xr-x 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/low/._meta
-rw-r--r-- 3 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/low/__attrs__
drwxrwxr-x 0 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/low/data/
-rw-r--r-- 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/low/data/.___0.blp
-rw-r--r-- 50187 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/low/data/__0.blp
drwxrwxr-x 0 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/low/meta/
-rw-r--r-- 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/low/meta/._sizes
-rw-r--r-- 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/low/meta/._storage
-rw-r--r-- 54 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/low/meta/sizes
-rw-r--r-- 115 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/low/meta/storage
drwxrwxr-x 0 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/open/
-rw-r--r-- 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/open/.___attrs__
-rwxr-xr-x 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/open/._data
-rwxr-xr-x 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/open/._meta
-rw-r--r-- 3 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/open/__attrs__
drwxrwxr-x 0 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/open/data/
-rw-r--r-- 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/open/data/.___0.blp
-rw-r--r-- 50208 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/open/data/__0.blp
drwxrwxr-x 0 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/open/meta/
-rw-r--r-- 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/open/meta/._sizes
-rw-r--r-- 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/open/meta/._storage
-rw-r--r-- 54 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/open/meta/sizes
-rw-r--r-- 115 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/open/meta/storage
drwxrwxr-x 0 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/volume/
-rw-r--r-- 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/volume/.___attrs__
-rwxr-xr-x 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/volume/._data
-rwxr-xr-x 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/volume/._meta
-rw-r--r-- 3 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/volume/__attrs__
drwxrwxr-x 0 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/volume/data/
-rw-r--r-- 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/volume/data/.___0.blp
-rw-r--r-- 82397 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/volume/data/__0.blp
drwxrwxr-x 0 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/volume/meta/
-rw-r--r-- 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/volume/meta/._sizes
-rw-r--r-- 222 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/volume/meta/._storage
-rw-r--r-- 54 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/volume/meta/sizes
-rw-r--r-- 115 example_data/root/data/test/2016-12-13T19;59;34.153449/daily_equities.bcolz/volume/meta/storage
drwxrwxr-x 0 example_data/root/data/test/2016-12-13T19;59;34.153449/minute_equities.bcolz/
-rw-r--r-- 222 example_data/root/data/test/2016-12-13T19;59;34.153449/minute_equities.bcolz/._metadata.json
-rw-r--r-- 141144 example_data/root/data/test/2016-12-13T19;59;34.153449/minute_equities.bcolz/metadata.json
-rw-r--r-- 965021 example_data/root/data/treasury_curves.csv
-rw-r--r-- 0 example_data/root/extension.py
```
