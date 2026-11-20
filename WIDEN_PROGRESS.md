# Widen progress: USEquityPricing (AL2) and the remaining 32-bit fields (AL3)

Branch `usequity-widen`, started at `master` = `dd2e9623`. Worktree:
`/home/yves/Documents/coding-projects/pluto-widen` (removed at the end; the branch stays).
Not pushed, not merged.

## How to run

```bash
cd <checkout of usequity-widen>
ENV=/home/yves/.cache/hades-pluto-dev/env36          # or a fresh one: dev/setup_env.sh <dir>
$ENV/bin/python setup.py build_ext --inplace -j 8 --force   # --force: a .pxd changed (see dev/README.md)
HOME=/home/yves/.cache/hades-pluto-dev/home-restore NOSE_PARAMETERIZED_NO_WARN=1 \
  $ENV/bin/python -m pytest -p no:cacheprovider -q \
  tests/test_pluto_examples.py tests/test_pluto_zipline_oracle.py \
  tests/test_pluto_isolation.py tests/test_pluto_assets.py tests/test_pluto_scenarios.py \
  tests/test_pluto_state.py
```

Diagnostics (outside the repo, not committed): `/home/yves/.cache/hades-pluto-dev/diag-widen/`.

## Checklist

- [x] **1 Baseline** (on `dd2e9623`): build ok (33 s), **55 passed** (57.1 s).
- [x] **2 AL2** USEquityPricing (Evidence 2), commit `6cf4eefd`.
- [x] **AL5** (owner decision, added during the run): `pluto/finance/metrics/controller_tracker.py`
  deleted, one commit `3e00fa83` (Evidence AL5).
- [x] **3 AL3** widen the remaining fields, fractional position through save/restore (Evidence 3),
  commit `e796492a`. Whole set then: **58 passed** (55.6 s) = 55 + 1 scenario + 2 state tests.
- [x] **4 Compatibility** (Evidence 4), commit `bcc95d09`: state from the old modules refused with
  a `ValueError`; `tests/test_pluto_state.py` 5 passed.
- [x] **6 Docs**: `dev/README.md` (environment, regeneration, `--force` rebuild, compatibility,
  tests, known differences).
- [x] **5 Proof** (on `14cae7b5`, code identical to the head): whole set (61 tests: the 55
  original, `pipeline_us_equity_pricing`, 5 in `tests/test_pluto_state.py`) 3 consecutive runs in the
  worktree: **61 passed** each (56.1 s, 57.7 s, 62.9 s; logs `diag-widen/det/`). Fresh clone:
  `git clone --branch usequity-widen` into `/home/yves/.cache/hades-pluto-dev/scratch-widen/`,
  `HOME=<scratch> dev/setup_env.sh <scratch>/env36` rc=0 in 76 s (extensions built), whole set
  **61 passed** in 57.9 s, `import pluto` ok, `install-examples` rc=0, clone `git status` clean
  before and after. Scratch deleted. `git diff --stat master`: 26 files, +1400 -507 (most in
  regenerated `*_pb2.py`). Later commits only touch this file.

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

### AL5. controller_tracker.py

`git grep -n "controller_tracker\|CalendarTracker"` before deleting: only the file itself and
`ALIGN_PROGRESS.md` (history, kept). Deleted `pluto/finance/metrics/controller_tracker.py`
(`CalendarTracker`, `MetricsTracker`; 161 lines): unused and half-finished, superseded by the
metrics-separation plan; `TestMetricsTracker` (`pluto/test_utils/test_utils.py`) is the working
implementation of the controller's tracker interface. After: `import pluto` ok,
`python __main__.py install-examples` into a temporary HOME rc=0 (`~/.pluto` with data,
launchers, metadata.sqlite, setup, strategies, templates), temporary HOME deleted.

### 3. Field inventory and the fractional position

Widened to `double` (every `float` in `protos/*.proto`; `grep float protos/*.proto` is empty;
all descriptors checked: no `TYPE_FLOAT` field left):
- `protocol.proto`: `Position.amount` (was `int64`), `cost_basis`, `last_sale_price`; `Account`
  (all 18); `Portfolio` (`cash_flow`, `starting_cash`, `portfolio_value`, `pnl`, `returns`, `cash`,
  `positions_value`, `positions_exposure`).
- `metrics.proto` (monitor packets): `CumulativeMetrics` (16 values), `PeriodMetrics` (7),
  `CumulativeRiskMetrics` (`algo_volatility`, `algorithm_period_return`, `max_leverage`,
  `excess_return`, `treasury_period_return`; the commented-out ones too).
- `ledger_state.proto`: `Return.value`.
- `controllable.proto`: `ControllableState.capital`, `max_leverage`;
  `ParametersUpdateRequest.capital`, `maximum_leverage`.
- `broker.proto`: `Commission.cost`; `OrderParams.amount`, `limit_price`, `stop_price` and the
  mistyped `limit_order`, `exchange` (unused message, only in commented code).
- `development.proto`: `SetupRequest.capital`, `max_leverage` (dev CLI messages).
- `params.proto`: `RunParams.capital_base` (no importer; its module was generated from
  `contrib/coms/protos/params.proto`, now from `protos/params.proto`).
- `slippage.proto`: `volume_limit`, `basis_points`, `price_impact`, `eta` map values.

Left as integers (identifiers, counts, calendar values, flags): `assets.Asset.sid` (int32),
`broker.AssetPrice.sid` (uint32), `broker.BrokerState.not_running` and
`controllable.ClockUpdateResponse.not_running` (uint32 flags), `protocol.Order.broker_order_id`
(int64 id), `metrics.longs_count`/`shorts_count` (int64 counts), `metrics.trading_days`,
`ledger_state.session_count`, `controllable.ControllableState.look_back`,
`development.SetupRequest.look_back` (int32 counts), `controllable.FractionalTradeSetup.minimal_fraction`
(int32: number of decimals amounts are rounded to, `broker_setup.py`), `calendar.proto` (`n`,
`year`, `month`, `day`, `days_of_week`, `hour`, `minute`, `second`, `TimeDelta.value`,
`open_offset`). Also left: `params.RunParams.maximum_leverage` is a `string` (unused message).

Regenerated with `$ENV/bin/python -m grpc_tools.protoc -I. --python_out=. protos/<name>.proto`
(grpcio-tools 1.33.2): `broker`, `controllable`, `development`, `ledger_state`, `metrics`,
`params`, `protocol`, `slippage`. `ledger_state.proto` imported `"protocol.proto"` (its module
had been generated with `protos/protocol.proto`); changed to `"protos/protocol.proto"` so it
compiles from the root. Every `protos/*_pb2.py` imports except `hub_pb2` and `monitor_pb2`,
which fail identically on `master` (descriptor errors in their own `.proto`; not touched).

**Surprise: fractional positions were truncated in memory, not only in saved state.**
The first run of the state test failed before any serialisation: after a 2.5-share fill the
position tracker held `2` (`AssertionError: 2 != 2.5`). `zipline/_protocol.pxd` declares
`InnerPosition.amount` as `np.int64_t`; every position (zipline's `Position` and Pluto's
`pluto/finance/position.py`, both wrap an `InnerPosition`) truncated a fractional amount.
Fix: `np.float64_t`. Integer amounts give identical arithmetic (exact in float64; Python and
the Cython stats loop already converted the integer to double when multiplying by a price), and
all 55 original tests still pass exactly; position amounts now come out as `10.0` instead of `10`
(compared by value everywhere). The build does not rebuild `_finance_ext` (which `cimport`s
`InnerPosition`) when only the `.pxd` changes: `build_ext --force` needed once (dev/README.md).

**Saved state did not work at all on master** (never called on the tested path):
`Ledger.get_state` raised `AttributeError` (`self.daily_returns_array` does not exist on the
ledger; `self._sessions` is never filled), `LedgerState` had no positions, `to_proto_account`
returned an empty `Account`, `to_zp_portfolio` returned `None` (and read a non-existent
`position` field), `to_zp_account` was `pass`, `Ledger.restore_state` stored a protobuf
`Timestamp` as the start date, and `MetricsTracker.restore_state` read a non-existent field
(`account_state`). Fixed: `LedgerState.positions` (field 8) written from the position tracker
(with `last_sale_date`) and restored through `PositionTracker.update_position`; the up-to-date
portfolio and account (the properties) written, restored as new objects with the portfolio's
positions bound to the tracker's store, not marked dirty (recomputing would compound
`returns` once more); `gross_leverage` restored from `leverage` (the ledger keeps them equal);
daily returns no longer written (the ledger has none; the field stays). Not saved (unchanged):
unpaid dividends and futures payout prices of the position tracker, `_previous_total_returns`
(reset at the next session start).

In-memory fractional scenario: not possible. There is no `FractionalEquity` asset type in the
tree; fractional orders depend on `BrokerSetup.fractional`, which the controllable takes from a
fresh `StrategySetup` whose `fractional` is always `False` (`server.py` `Initialize`; the test
harness's own `bs.fractional = True` is never used), and Zipline (the oracle) casts order
amounts to `int` (`zipline/algorithm.py` `_calculate_order_amount`). So: state round trip only.

`tests/test_pluto_state.py`: real `MetricsTracker` over a real `Ledger` and `PositionTracker` (as
`Controllable.initialize` builds them), a reader-less `DataPortal` for the calendar; fills of
2.5 AAPL at 543.2109876, 0.125 MSFT at 37.123456789, -0.0625 MSFT at 38.987654321, last sale
prices set with `PositionTracker.update_position`, a commission of 1.0123456789 and an open
order of 1.75 shares (limit 38.123456789, filled 0.3333). `get_state` -> fresh tracker
`restore_state`: positions (amount, cost basis, last sale price and date, compared by
`float.hex`), portfolio (all fields and its positions), account (18 fields), position stats,
order fields equal, and the restored tracker's `get_state` returns the same bytes. The prices,
cost bases, cash and portfolio value are asserted not representable in 32 bits. Second test:
the monitor packet (`to_proto_performance_packet` -> bytes -> `from_proto_performance_packet`)
returns every value, the 2.5-share position and the transaction unchanged.
Master's modules, for comparison (scratch package with master's `*_pb2.py`):
`Position(amount=2.5)` -> `TypeError: 2.5 has type float, but expected one of: int, long`;
`cost_basis` 543.2109876 comes back as 543.2109985351562.

### 4. Compatibility

Under the `double` definitions, a `float` value written by the old modules (wire type 5) does
not match the field's wire type (1) and parses as an unknown field: the value reads as 0, with
no error (test `test_old_values_would_be_lost`, bytes written by master's modules: `cash` 1000.5
reads 0.0, `session_count` 3 survives). Handling: `LedgerState.version` (field 9, `uint32`),
written as `pluto.finance.ledger.STATE_VERSION = 1`; `Ledger.restore_state` raises
`ValueError('cannot restore the ledger state: format version 0, expected 1. State saved before
the protobuf fields were widened to 64 bits (no version) cannot be read ...')` for anything else.
Tested directly and through `MetricsTracker.restore_state` (old ledger bytes inside a
`TrackerState`). `ControllableState` has no version: nothing restores it
(`Controllable.restore_state` raises `NotImplementedError`; documented in dev/README.md, as
the slippage parameter files, which nothing in the tree writes).

## Next step

All steps done. Owner items: fractional trading on the in-memory path (no oracle; `StrategySetup.fractional`
never set); `ControllableState` restore not implemented; `hub_pb2`/`monitor_pb2` do not import
(pre-existing, unchanged).
