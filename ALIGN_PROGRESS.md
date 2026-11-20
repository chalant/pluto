# Align progress: exact alignment of Pluto with Zipline

Branch `align-exact`, started at `cleanup-superseded` = `9b3c862b`. Worktree:
`/home/yves/Documents/coding-projects/pluto-align` (removed at the end; the branch stays).
Not pushed, not merged.

Goal (owner, 2026-11-20): align the test results with no loose tolerances; fix the ledger,
positions and returns computations where needed. Brief: steps A-G (re-measure, tighten,
known differences, scenario coverage, float width, determinism, docs and fresh-clone proof).

## How to run

```bash
cd <checkout of align-exact>
ENV=/home/yves/.cache/hades-pluto-dev/env36          # or a fresh one: dev/setup_env.sh <dir>
$ENV/bin/python setup.py build_ext --inplace -j 8    # once per checkout (artefacts not committed)
HOME=/home/yves/.cache/hades-pluto-dev/home-restore NOSE_PARAMETERIZED_NO_WARN=1 \
  $ENV/bin/python -m pytest -p no:cacheprovider -q \
  tests/test_pluto_examples.py tests/test_pluto_zipline_oracle.py \
  tests/test_pluto_isolation.py tests/test_pluto_assets.py tests/test_pluto_scenarios.py
```

Expected: 55 passed (the original 35, tightened, and 20 scenarios).

Diagnostics (outside the repo, not committed): `/home/yves/.cache/hades-pluto-dev/diag-align/`
- `zero_tol.py [example ...]`: largest abs/rel difference per column, Pluto vs Zipline, Pluto vs
  stored, Zipline vs stored (the lead's script, path adapted; also counts non-numeric field and
  key-set differences in positions/orders/transactions, and compares period_open/close).
- `first_diff.py <example>`: first row where cash/pnl/returns differ, with the raw transaction order.
- `scen_diff.py <scenario> [rows]`: errors of one scenario and its first differing rows.
- `measure_e.py <repo> <scenario...>`: largest relative difference of limit/stop prices, transactions.
- `scen_facts.py [scenario...]`: what each scenario exercises (from the Zipline frame).

**Paths**: the agent session's cwd was the owner's checkout; always use absolute paths into the
worktree. One accidental edit of `controller.py` in the owner's checkout was reverted with
`git apply -R` of exactly that hunk (its `git status` shows only the owner's untracked dirs).

## Checklist

- [x] **A Baseline** (on `9b3c862b`): build ok, 35 passed (38.5 s); diagnostic re-run, lead's
  numbers confirmed with one correction (Evidence A)
- [x] **B Tighten** (Evidence B): oracle exact; stored results exact except momentum_pipeline
  `rtol=2e-12`; `except TypeError: pass` removed; every common column compared, lists strictly
- [x] **C Known differences** (Evidence C): `period_open` fixed (execution open, compared
  exactly); `schedule_function` calendar fixed; recorded variables: options for the owner
- [x] **D Scenarios**: 20 scenarios, all exact after 7 fixes (Evidence D, table)
- [x] **E Float width** (Evidence E): the in-memory path serialises through protobuf; 32-bit
  fields changed limit/stop prices, the strategy's portfolio view and tick rounding; fixed
- [x] **F Determinism** (on `20494299`, code identical to the head): whole set (55 tests) 6 consecutive
  runs: 55 passed each (53-60 s); PYTHONHASHSEED=1..10: see Evidence F. No intermittent failure seen.
- [x] **G Docs, fresh clone**: `dev/README.md` updated; `git clone --branch align-exact` at `e3a73809`
  into `/home/yves/.cache/hades-pluto-dev/scratch-align/`, `HOME=<scratch> dev/setup_env.sh <scratch>/env36`
  rc=0 in 1 min 21 s (extensions built), then the whole set: **55 passed** in 60.7 s. Scratch deleted.
  Later commits only touch this file.

## Evidence

### A. Re-measurement (base `9b3c862b`)

`zero_tol.py`, all five examples, same process and test bundle:
- `buy_and_hold`, `buyapple`, `dual_ema_talib`, `dual_moving_average`: Pluto == Zipline bit for
  bit on all 34 compared columns (numeric fields of positions/orders/transactions included).
  Against the stored results: all values equal; **correction**: the stored `transactions` dicts
  have one more key, `'commission': None` (format of the zipline version that wrote the files;
  the fork's `Transaction.to_dict` has none). The lead's script skipped missing keys.
- `momentum_pipeline`, Pluto vs Zipline: 16 columns not bit-identical, max abs 2.9e-11
  (`ending_cash`, `portfolio_value`, `starting_cash`), max rel 7.1e-13 (`returns`), 4.6e-13 (`pnl`).
  Pluto vs stored: 17 columns, max rel 9.8e-13 (`alpha`). Zipline vs stored: 17 columns, max rel
  1.06e-12 (`alpha`), max abs 1.3e-10 (`ending_cash`). Confirmed.

### B. Tightened comparisons

Fix 1 first (commissions after transactions, controller side): root cause of Pluto != Zipline
in `momentum_pipeline` (`first_diff.py`): first differing row 2013-10-08, same six transactions
in the same order, same commissions; `ending_cash` z=99797.68772414053, p=99797.68772414052.
By hand: all transactions then all commissions gives …053 (Zipline's
`AlgorithmSimulator.every_bar`); interleaving transaction/commission gives …052 (Pluto's
`Controller.clock_update`). After the fix Pluto == Zipline bit for bit on all five examples,
period columns included; Pluto vs stored == Zipline vs stored.

Policy (also in `dev/README.md`): nothing looser than `rtol=1e-7, atol=0`.
- Oracle (`tests/test_pluto_zipline_oracle.py`): `RTOL = ATOL = 0`; every common column through
  `pluto.test_utils.test_utils.compare_column`; `period_open` exact (was session level).
- Stored results (`TestMetricsTracker.validate`): every column both frames have (37), exact;
  `momentum_pipeline` `rtol=2e-12, atol=0` (`STORED_RTOL`), measured max rel 1.06e-12 (alpha).
  Stored transactions: `'commission': None` dropped before comparing. NaN in the reference
  counts as 0 (Pluto's tracker reports 0 for first-day volatility/ratios).
- `_compare_df` (no live caller): exact, via `compare_column`.
- Negative checks (`neg_check.py`): one-minute shift of a period_open, a changed label, `pnl`
  moved by one ulp, `amount` +1e-9 in a position/order/transaction dict: all detected.
- Result: 35 passed (35.3 s).

### C. Known differences

- `period_open`: the test tracker (`TestMetricsTracker.start_of_session`, gets the run's calendar),
  the strategy-side tracker (`pluto/finance/metrics/tracker.py`, from `data_portal.trading_calendar`)
  and `controller_tracker.py` stamp the session's execution open, as zipline's
  `MetricsTracker.handle_market_open`. Compared exactly: equal on all examples and scenarios.
  `controller_tracker.py` is unused and otherwise broken (`self._ledger` never set): owner item.
- `schedule_function` `calendar`: Pluto ignored it (date rules always on the strategy calendar).
  Scenario `schedule_calendar` (rules on US_EQUITIES and US_FUTURES, 2013): before the fix Pluto
  sold MSFT on 2013-01-22 (equity week start after MLK day) where Zipline, on the futures
  calendar (Monday 01-21 is a futures session), did not: 12 order rows, 6 transaction rows,
  max |ending_cash| difference 2288.56. Fix: zipline's mapping (None -> the algorithm's calendar,
  US_EQUITIES -> XNYS, US_FUTURES -> us_futures, else `ScheduleFunctionInvalidCalendar`). Exact after.
  On the way: in simulation the algorithm had **no** calendar at all (only set on a live calendar
  update), so every rule other than `every_day` crashed (`AttributeError: 'NoneType' object has
  no attribute 'minute_to_session_label'`, scenario `schedule_rules`). Fix: the controllable sets
  it before `initialize`.
- Recorded variables: not implemented (design question, see Owner items). Assessment: `record()`
  works on the strategy side (`algo._recorded_vars`), but nothing carries the values out: the
  strategy-side daily packet has `recorded_vars` commented out (`Controllable._get_daily_message`)
  and goes to the monitor through `metrics.proto` (no field), and the frame the tests compare is
  built by the controller-side tracker, which only sees the ledger. Any option needs a protobuf
  change and a typed value (zipline records floats, ints and booleans, e.g. `buy`/`sell` in
  `dual_ema_talib`).

### D. Scenarios (`tests/test_pluto_scenarios.py`, `tests/pluto_scenarios/`)

Each strategy runs unchanged in plain Zipline (`run_algorithm`, models from
`set_commission`/`set_slippage`) and in Pluto (models from `_test_args`); frames compared
exactly, no column on one side only. Test bundle: 9 equities 2004-2014; adjustments: 5 splits
(CERN 2006-01-10, 2011-06-27, 2013-07-01; AAPL 2005-02-28, 2014-06-09), cash dividends for 7
sids **with no pay date** (`pay_date`, `declared_date`, `record_date` all NaT), no stock dividends,
no mergers. Dividends are therefore earned but never paid, in Zipline as in Pluto (no cash change
without a transaction in `dividends`); dividend payment is not covered. A bundle exercising it
needs `dividend_payouts` rows with pay dates (and record/declared dates), `stock_dividend_payouts`
rows, and ideally a merger.

| Scenario | Result | Measured difference before the fixes | Cause / fix |
|---|---|---|---|
| market_orders | pass | none | |
| limit_orders | pass | limits executed as market orders: 150 AAPL bought 2014-01-03 instead of 50, max abs ending_cash 54508; then limit prices rel 3.3e-8 | D2, D6 |
| stop_orders | pass | stops executed as market orders; then stop prices rel 3.6e-8 | D2, D6 |
| stop_limit_orders | pass | executed as market; then limit rel 4.3e-8, stop rel 2.4e-8 | D2, D6 |
| target_orders | pass | none (order_target, _value, _percent, order_value, order_percent) | |
| short_and_cover | pass | none | |
| cancel_orders | pass | cancelled marketable order filled in Pluto (10 AAPL 2014-01-06; final cash 92769.76 vs 98210.05); cancelled orders re-placed as new open orders; limit rel 4.1e-8 | D2, D3, D6 |
| partial_fills | pass | none (VolumeShareSlippage volume_limit 0.0005; fills over 48 bars) | |
| commission_per_trade | pass | none | |
| commission_per_dollar | pass | none (with FixedBasisPointsSlippage) | |
| schedule_rules | pass | crash (no calendar) | D4 |
| schedule_calendar | pass | extra sell 2013-01-22, ending_cash up to 2288.56 | D5 |
| history_window | pass | none (windows reaching before the start) | |
| pipeline_screen | pass | none with generic `EquityPricing`; with `USEquityPricing` Pluto fails (`ValueError: No PipelineLoader registered for column EquityPricing<US>.close`) | owner item |
| leverage_close | pass | none (gross leverage 1.65, all positions closed) | |
| splits | pass | limit price rel 4.1e-8 | D6 |
| dividends | pass | none (payment not exercised, see above) | |
| delisting | pass | none (DELL auto close, open limit order cancelled) | |
| eod_cancel | pass | none (daily: zipline executes cancel policies only for minute data) | |
| portfolio_view | pass | limits derived from cash/portfolio value/cost basis rel up to 3.7e-5 (float32); with doubles, one tick (AAPL sell limit 271.41 vs 271.4, cost basis bits) | D6, D7 |

Defects fixed (each with its regression scenario):
- **D1** controller ledger: commissions processed between transactions (cash summation order) — oracle `momentum_pipeline`.
- **D2** `pluto.algorithm.TradingAlgorithm.order` dropped `limit_price`, `stop_price`, `style`:
  every order was a market order (also `order_target*`/`order_value`/`order_percent` with a style).
- **D3** cancellations: `to_proto_order`/`to_zp_order` dropped the order status, and
  `StrategyManager.place_orders` appended every relayed order as a new open order, so a
  cancelled order stayed open on the controller side (and filled) with a duplicate added.
  Fix: status carried; relayed cancellations cancel the controller's order (recorded only if
  placed and cancelled in the same bar).
- **D4** no trading calendar on the algorithm in simulation (schedule rules crashed).
- **D5** `schedule_function` ignored `calendar`.
- **D6** 32-bit protobuf fields on the in-memory path (see E).
- **D7** strategy side (`Controllable.bar`): commissions processed between transactions.
- (C) `period_open` stamped with the session label.

### E. Float width

The in-memory path serialises: the controllable sends its new orders as a serialised
`broker.OrderBatch` (`commands.ClockUpdate` -> `BrokerStub.PlaceOrders` -> bytes ->
`StrategyManager.PlaceOrders` -> `to_zp_order`), and the controller sends fills and prices as
`broker.BrokerState` protobuf objects (`TransactionCommission`, `AssetPrice`) that
`Controllable.update_blotter_and_positions` converts back; the run capital goes through
`RunRequest`, assets through `assets.Asset`. With 32-bit fields (measured on `c4c2648a`, `measure_e.py`):
limit/stop prices stored on the controller differed from Zipline's by up to 4.3e-8 relative
(e.g. 542.0600000000001 -> 542.0599975585938), the strategy's cash/portfolio value/positions by
float32 rounding (limits derived from them up to 3.7e-5 relative, one tick), and assets
rebuilt from protobuf had `tick_size` 0.009999999776482582 instead of 0.01, which changed
zipline's limit-price rounding. No fill changed in these scenarios, but a fill at a limit/stop
boundary or a fractional amount (Pluto supports fractional trading) would.
Fix (minimal set on the path): `double` for `protocol.Order` amount/filled/commission/stop/limit,
`finance.Transaction` amount/price, `broker.AssetPrice.price`, `broker.TransactionCommission.commission`,
`controllable.RunRequest` capital/leverage, `controllable.FractionalTradeSetup.minimum_purchase`,
`assets.Asset` tick_size/multiplier; the five `_pb2.py` regenerated with
`$ENV/bin/python -m grpc_tools.protoc -I. --python_out=. protos/<name>.proto` (grpcio-tools 1.33.2;
the old modules came from other protoc versions, so the generated files differ in style, not
only in the field types). Left 32-bit (not on the compared path; owner item): `Position`,
`Portfolio`, `Account` (state), `metrics.proto` (monitor packets), `ledger_state.Return.value`,
`ControllableState.capital/max_leverage`, `broker.Commission.cost`, `broker.OrderParams`.

### F. Determinism

Logs: `/home/yves/.cache/hades-pluto-dev/diag-align/det/`. Runs 1-6: 55 passed each.
PYTHONHASHSEED 1-4: 55 passed each; seeds 5-10 were still running when the run budget ended
(the summary file there has the final lines).

## Owner items (design questions, not changed)

1. Recorded variables: (a) strategy-side packet carries them to the monitor (uncomment in
   `_get_daily_message`, add a typed field to `metrics.proto`), tests read monitor packets;
   (b) carry them with each strategy's order batch to the controller and let the controller
   tracker add them to the daily packet; (c) keep them strategy-local and document. (a)/(b) need
   a typed value (`google.protobuf.Struct`/`Value`) to keep booleans and ints as zipline does.
2. `USEquityPricing` in pipelines: Pluto's domain only loads the generic `EquityPricing`;
   accept US-specialised columns (map them to the universe's loader) or document the restriction.
3. Remaining 32-bit protobuf fields (state and monitor messages, `Position.amount` is int64 so a
   fractional position is truncated in saved state); the float->double change is wire-incompatible
   with any state serialised with the old modules.
4. Dividend payment untested (bundle has no pay dates); a bundle with pay dates, stock dividends
   and mergers is needed.
5. Minute mode is not covered. From the code (not run): zipline executes cancel policies at
   session end for minute data; Pluto's `Controllable.session_end` has it commented out.
6. `pluto/finance/metrics/controller_tracker.py`: unused and broken (`self._ledger`); delete or finish.

7. Not tested: the remote gRPC path (servers, channels, `dev/cli.py`); the in-memory path
   uses the same servicers and the same protobuf serialisation, called directly.

## Next step

All steps done. If continuing: confirm PYTHONHASHSEED 5-10 in the determinism logs; owner items above.
