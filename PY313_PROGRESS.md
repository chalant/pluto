# PY313 progress: port Pluto to Python 3.13 / numpy 2.5.4 / pandas 2.3.x (approach B)

Branch `py313-port` (from `master` `8385cf15`), worktree `/home/yves/Documents/coding-projects/pluto-py313`.
Not pushed, not merged. Scratch: `/home/yves/.cache/hades-pluto-dev/py313/port/`
(`env313` = the Python 3.13.16 venv, `zr` = clone of zipline-reloaded checked out at tag 3.2.1,
`pins-initial.txt` = the resolved set from `PyDeps/res_b_pandas_lt3.txt`, logs).
Studies (read-only): `/home/yves/.cache/hades-pluto-dev/py313/{PyPort,PyDeps,PyScan}/REPORT.md`.

## How to build and test (new stack)

```bash
WT=/home/yves/Documents/coding-projects/pluto-py313
P=/home/yves/.cache/hades-pluto-dev/py313/port
(cd $WT && HOME=$P/home $P/env313/bin/python setup.py build_ext --inplace -j 8)   # ~35 s
# zipline-reloaded's own tests against the vendored package (C1):
(cd $P/zr && PYTHONPATH=$WT HOME=$P/home $P/env313/bin/python -m pytest -o cache_dir=$P/pytestcache -p no:randomly \
   tests/test_examples.py tests/finance/test_transaction.py tests/test_algorithm.py -k 'not talib' -q)
```

(Do not pass `-p no:cacheprovider` to those: zipline-reloaded's `addopts` has `--ff`.) Old-stack reference runs
of `origin/master` only: see the brief (`env36`).

## Checklist

- [x] worktree + branch, scratch environment `env313` (`uv pip sync pins-initial.txt`, rc=0)
- [x] S1 vendoring commit `e367db53` "Base: zipline-reloaded 3.2.1 (tag 3.2.1), Apache-2.0" (`zipline/` replaced by
  `src/zipline` of the tag, `zipline/VENDORED.txt` added; `zipline/__init__.py` is upstream's, PM15)
- [x] build commit `82ef0156`: `setup.py` rewritten (extension list of 3.2.1 on the flat layout, plus
  `pluto.control.clock.sim_engine`, no duplicate int64 window, no distutils/versioneer, `language_level="3"`),
  `etc/requirements.txt` loose modern set, dead extras files removed (PM13)
- [x] **C1a** plain vendored 3.2.1 builds (Cython 3.3.1, numpy 2.5.4, setuptools 84.2.1, 17 `.so`, 35 s, rc=0,
  0 lines with "error") and zipline-reloaded's `tests/test_examples.py`, `tests/finance/test_transaction.py`,
  `tests/test_algorithm.py` (`-k 'not talib'`): **130 passed, 1 skipped in 41.75 s** (`c1-plain.log`)
- [ ] re-apply Pluto's changes (one commit each, message cites the Pluto commit(s), "originally against zipline
  bbf92210", the K-number; see "Re-apply plan")
- [ ] **C1b** same three zipline-reloaded test files after the re-applied changes
- [ ] S2: port `pluto/` (calendars, `collections.abc`, SQLAlchemy 2, `sim_engine.pyx`, protos, deletions PM23)
- [ ] S3: test harness; expected results (PM19)
- [ ] mirror zipline-reloaded behaviour fixes into Pluto's copies (PM17)
- [ ] `dev/setup_env.sh`, `dev/README.md`, `etc/requirements-py313.txt` (exact pins)
- [ ] C2 .. C7

## Re-apply plan (S1), decided after reading the PyPort merges

Semantic changes only; cosmetic ones (blank lines, `set([..])`, `FROM splits`, `embedsignature(True)`, the `_calls`
slot in `Position` (unused), `== 0.0`) are dropped. Reference merges: `PyPort/merged-3.2.1-black/zipline/`.
Files modified get a "Modified by Pluto" header line (Apache-2.0 section 4(b)).

1. K77: `zipline/_protocol.pxd` `InnerPosition.amount` int64 -> float64
2. K1: `zipline/assets/_assets.pyx` `cdef class FractionalEquity(Equity): pass`
3. fractional transactions: `finance/transaction.py` (no `int()`, no minimum magnitude; keep `py["commission"] = None`: upstream's)
4. `protocol.py` `Positions(OrderedDict)`
5. `data/data_portal.py` `if ffill and field == "price"`
6. `data/dispatch_bar_reader.py` compare `trading_calendar.name`
7. `utils/sqlite_utils.py` `check_same_thread=False`
8. K28: `finance/order.py` (`session_id` first arg, `id_`, `__hash__`/`__eq__` by id; keep upstream split fix `62cb4a13`),
   `finance/blotter/blotter.py`, `simulation_blotter.py`
9. K30: `pipeline/engine.py` `controllable` replaces `asset_finder`; `algorithm.py` passes `self`
10. K65: `algorithm.py` `before_trading_start(self, algo, data)`, `gens/tradesimulation.py`, `utils/api_support.py`
    (register on `pluto.api` and `zipline.api`)
11. `finance/metrics/tracker.py` injected `ledger=None`
12. calendar per run (after the `pluto/trading_calendars` port): `data/bcolz_daily_bars.py`, `data/bcolz_minute_bars.py`,
    `pipeline/domain.py`, `data/bundles/core.py` (`pluto.assets.assets.AssetFinder` in `load()`)

## Decisions

- Version: no versioneer; `setup.py` has a static `0.1.0.dev0`. `zipline/_version.py` does not exist (upstream's
  `__init__` falls back to "unknown version").
- Cython 3.3.1 (not `<3.2.0` as zipline-reloaded's build-system) builds the vendored tree without errors.
- `FractionalEquity` is unused anywhere in the tree; re-applied only because PyPort classed it as a feature.

## Current failing point / exact next step

Re-apply step 1 (K77) and continue down the list above; the regions to edit were read at the end of the previous
session (`_protocol.pxd:14`, `transaction.py:63-75`, `sqlite_utils.py:40`, `dispatch_bar_reader.py:49-57`,
`protocol.py:267`, `data_portal.py:915`). After 1-11, rebuild with `--force` is automatic (setup.py forces cythonize).

## Open owner items

(none yet)
