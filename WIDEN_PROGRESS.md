# Widen progress: USEquityPricing (AL2) and the remaining 32-bit fields (AL3)

Branch `usequity-widen`, started at `master` = `dd2e9623`. Worktree:
`/home/yves/Documents/coding-projects/pluto-widen` (removed at the end; the branch stays).
Not pushed, not merged.

## How to run

```bash
cd <checkout of usequity-widen>
ENV=/home/yves/.cache/hades-pluto-dev/env36          # or a fresh one: dev/setup_env.sh <dir>
$ENV/bin/python setup.py build_ext --inplace -j 8    # once per checkout (artefacts not committed)
HOME=/home/yves/.cache/hades-pluto-dev/home-restore NOSE_PARAMETERIZED_NO_WARN=1 \
  $ENV/bin/python -m pytest -p no:cacheprovider -q \
  tests/test_pluto_examples.py tests/test_pluto_zipline_oracle.py \
  tests/test_pluto_isolation.py tests/test_pluto_assets.py tests/test_pluto_scenarios.py
```

Diagnostics (outside the repo, not committed): `/home/yves/.cache/hades-pluto-dev/diag-widen/`.

## Checklist

- [x] **1 Baseline** (on `dd2e9623`): build ok (33 s), **55 passed** (57.1 s).
- [x] **2 AL2** USEquityPricing (Evidence 2).
- [ ] **3 AL3** widen the remaining fields, fractional position through save/restore.
- [ ] **4 Compatibility** of state saved with the old modules.
- [ ] **5 Proof**: whole set, fresh clone, 3 consecutive runs.
- [ ] **6 Docs**: `dev/README.md`, this file.

## Evidence

### 2. USEquityPricing

Cause: the controllable gives the engine Pluto's own domain (`pluto.pipeline.domain.Domain`,
country US, the run's calendar) as default domain, and the universe's loader
(`TestUniverse.pipeline_loader`, `pluto/data/universes/core.py`) accepted only
`EquityPricing.specialize(<that domain>)` columns. A `USEquityPricing` column is
`EquityPricing<US_EQUITIES>`: the pipeline resolves to zipline's `US_EQUITIES` domain (as in
Zipline, where it is the default domain for XNYS) and the loader refused it:
`ValueError: No PipelineLoader registered for column EquityPricing<US>.high::float64.`
(new scenario, before the fix).

Fix: the loader also accepts an `EquityPricing` column specialised to another domain of the
universe's country (`column.unspecialize() in EquityPricing.columns` and same
`country_code`); the same `USEquityPricingLoader` serves it. The engine then runs that pipeline
on `US_EQUITIES`, exactly as Zipline's `run_algorithm` does. Generic `EquityPricing` columns
take the unchanged first branch.

Scenario `pipeline_us_equity_pricing` (registered automatically: `tests/pluto_scenarios/__init__.py`
loads every module of the directory): `AverageDollarVolume`, `SimpleMovingAverage`, `Returns` and a
`CustomFactor` (`CloseLocation`, inputs open/high/low/close) on `USEquityPricing`, a screen (top 5
by dollar volume and close average above 20), a rank with a mask; weights from rank and the custom
factor. Zipline side: 60 days, 269 transactions, 4-5 screened assets per day. Pluto vs Zipline:
exact on all 37 common columns, no column on one side only.

## Next step

Step 3 (AL3).
