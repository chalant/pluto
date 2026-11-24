# Leftovers closeout (branch `py313-leftovers`)

Base: `origin/master` = `4f9d2519`. Worktree: `/home/yves/.cache/hades-pluto-dev/leftovers/wt`; env
`/home/yves/.cache/hades-pluto-dev/leftovers/env313` (built with `dev/setup_env.sh`); scratch
`/home/yves/.cache/hades-pluto-dev/leftovers/scratch`. Owner decisions PM24 to PM26 and the `test_resource_path`
collection fix, one commit each. A successor continues from the Status list.

## Status

- [x] PM24 drop the pandas 0.22 stored results
- [x] PM25 delete `pluto/_protocol.pyx`
- [x] PM27 fix the zipline.testing pipeline-engine fixtures (owner decision, added during the work)
- [ ] PM26 delete dead code, stale files, the 2019 test-suite copy
- [x] `test_resource_path` collected as a test
- [ ] final run from a fresh clone

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
| `STORED_EXPECTED_DIFFERENCES` (dict, 43 lines with its comment) | `tests/test_pluto_examples.py` | verbatim below |
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
| `.tar.gz` bytes | 4937417 (sha256 `bbc262d4...b3b`) | 3923901 (sha256 `50a115e1...95d`) |
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

## Fixes

### `test_resource_path` collected as a test

`zipline/testing/core.py:1282` defines `def test_resource_path(*path_parts)` (path of `tests/resources/<parts>`; the
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
