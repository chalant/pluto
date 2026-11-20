# Align progress: exact alignment of Pluto with Zipline

Branch `align-exact`, started at `cleanup-superseded` = `9b3c862b`. Worktree:
`/home/yves/Documents/coding-projects/pluto-align`. Not pushed, not merged.

Goal (owner, 2026-11-20): align the test results with no loose tolerances; fix the ledger,
positions and returns computations where needed. Brief: steps A-G (re-measure, tighten,
known differences, scenario coverage, float width, determinism, docs and fresh-clone proof).

## How to run

```bash
cd /home/yves/Documents/coding-projects/pluto-align
ENV=/home/yves/.cache/hades-pluto-dev/env36
$ENV/bin/python setup.py build_ext --inplace -j 8          # once per worktree (artefacts not committed)
HOME=/home/yves/.cache/hades-pluto-dev/home-restore NOSE_PARAMETERIZED_NO_WARN=1 \
  $ENV/bin/python -m pytest -p no:cacheprovider -q \
  tests/test_pluto_examples.py tests/test_pluto_zipline_oracle.py \
  tests/test_pluto_isolation.py tests/test_pluto_assets.py
```

Diagnostics (outside the repo): `/home/yves/.cache/hades-pluto-dev/diag-align/`
- `zero_tol.py [example ...]`: largest abs/rel difference per column, Pluto vs Zipline, Pluto vs
  stored, Zipline vs stored (copy of the lead's script, path adapted, also counts non-numeric
  field and key-set differences in positions/orders/transactions). `DIAG_OUT=<json>` sets the output.
- `first_diff.py <example>`: first row where cash/pnl/returns differ, with the raw transaction order.

**Paths**: the agent session's cwd is the owner's checkout; always use absolute paths into
`pluto-align` (one accidental edit of `controller.py` in the owner's checkout was reverted with
`git apply -R` of exactly that hunk; `git status` there shows only the owner's untracked dirs).

## Checklist

- [x] **A Baseline** (on `9b3c862b`, build ok)
  - [x] 35 passed (38.5 s)
  - [x] diagnostic re-run: lead's numbers confirmed (see Evidence A)
- [ ] **B Tighten**
- [ ] **C Known differences** (`period_open`, recorded variables, `schedule_function` calendar)
- [ ] **D Scenarios**
- [ ] **E Float width**
- [ ] **F Determinism**
- [ ] **G Docs, fresh clone**

## Evidence

### A. Re-measurement (base `9b3c862b`)

`zero_tol.py`, all five examples, same process and test bundle:
- `buy_and_hold`, `buyapple`, `dual_ema_talib`, `dual_moving_average`: Pluto == Zipline bit for
  bit on all 34 compared columns (numeric fields of positions/orders/transactions included).
  Against the stored results: all values equal; the only difference is the key set of the
  stored `transactions` dicts, which carry `'commission': None` (format of the zipline version
  that wrote the files; the fork's `Transaction.to_dict` has no `commission`). The lead's script
  skipped missing keys, so it did not see it.
- `momentum_pipeline`, Pluto vs Zipline: 16 columns not bit-identical, max abs 2.9e-11
  (`ending_cash`, `portfolio_value`, `starting_cash`), max rel 7.1e-13 (`returns`), 4.6e-13 (`pnl`).
  Pluto vs stored: 17 columns, max rel 9.8e-13 (`alpha`). Zipline vs stored: 17 columns, max rel
  1.06e-12 (`alpha`), max abs 1.3e-10 (`ending_cash`).

### Fix 1: commissions after transactions (controller ledger)

Root cause of Pluto != Zipline in `momentum_pipeline` (`first_diff.py`): first differing row
2013-10-08, same six transactions in the same order, same commissions; `ending_cash` z=99797.68772414053,
p=99797.68772414052. Recomputing by hand: transactions first, then commissions gives …053 (Zipline,
`AlgorithmSimulator.every_bar` processes all transactions, then all commissions); interleaving
transaction/commission gives …052 (Pluto's `Controller.clock_update`). Fix: the controller collects
the commissions and processes them after the transactions. After: Pluto == Zipline bit for bit on
all five examples (0 columns differ); Pluto vs stored == Zipline vs stored (max rel 1.06e-12, `alpha`).
