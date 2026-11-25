# Stage 1, engine: broker service, deterministic order ids, per-strategy event stream

Branch `stage1-engine` from `origin/master` `c096b1c6`. Worktree
`/home/yves/.cache/hades-pluto-dev/stage1/engine/wt`, env `.../engine/env`, scratch `.../engine/`
(`t.sh` runs pytest from the worktree, `seven.txt` lists the seven test files). Not pushed.
Gate: the seven test files pass unchanged, exact tolerances; no result changes.

## Map of the code before the change (step 1)

**Who computes fills.** `pluto/finance/markets/market_simulation.py` `MarketSimulation.get_transactions(calendar,
blotter)`: for every asset with open orders in the *controller side* blotter it runs the slippage model on the
calendar's `BarData`, the commission model, and mutates the order (`filled`, `commission`, `dt`). It returns
`transactions` (dicts `order`, `transaction`, `commission`) and the `closed_orders`. The controller (`Controller.
clock_update`, BAR/TRADE_END branch) calls it, then applies the result.

**Who writes the strategy's book (controller side).** `StrategyManager` (`pluto/control/controller/process_manager.py`)
holds one `protocol.Portfolio`, `Positions`, `Account` per run (shared by all calendars: stage 1 means one strategy per
manager), and per calendar a `PositionTracker`, a `Ledger` and a `SimulationBlotter` (the controller's copy of the open
orders, created by the caller: tests call `manager.add_ledger`, `add_blotter`, `add_data_portal`). The ledger writes happen
in these methods, called by the controller: `sync_last_sale_prices`, `process_transaction` (ledger.process_transaction +
process_order), `process_commission`, `place_orders` (blotter + ledger.process_order, cancels relayed by the strategy),
`handle_market_open` (ledger.start_of_session), `process_dividends`, `process_splits` (blotter + ledger),
`cleanup_expired_assets` (close positions, cancel open orders of expired assets, ledger.process_order). The strategy side
has its own replica book (`Controllable`: `MetricsTracker` over a `Ledger`, own `SimulationBlotter`); it is fed with the
fills in the `BrokerState` the controller sends each event (`account_update`) and is not part of this change.

**Where order ids are minted.** `zipline/finance/order.py` `Order.__init__`: `id = make_id() + session_id` when `id_` is
None, `make_id()` is `uuid4().hex`. The only caller on Pluto's path is `pluto/finance/blotter/simulation_blotter.py`
`SimulationBlotter.order` (the strategy's blotter; `session_id` = the strategy's process id = `md5(strategy_id +
session_id)`). `conversions.to_zp_order` rebuilds orders on the controller side from the protobuf with `id_=proto.id`, so
the controller never mints. `pluto/finance/blotter/controllable_blotter.py` and `live_blotter.py` are the live path (not
imported on the simulation path). Zipline's own `TradingAlgorithm` keeps random ids (the oracle).
Ids in the tests: `pluto/test_utils/test_utils.py` `_ID_FIELDS = {'id','order_id','session_id'}` are dropped by
`_normalize` before any comparison; no test compares an id (checked, see step 3).

**Calls across the controller / controllable boundary per clock event** (`Controller.clock_update(ts, evt, signals)`;
signals are the per-calendar `(cts, evt, calendar)` of that tick):

1. controller, per signal, before anything else: `market.set_calendar_timestamp`, `blotter.set_date(cts)`.
2. controller, per signal, by event (controller writes its book):
   - `BAR` / `TRADE_END`: `sync_last_sale_prices`; `market.get_transactions`; all `process_transaction`, then all
     `process_commission` (cash summation order), `broker_states[session].transactions.append(...)` (fills for the
     strategy); `blotter.prune_orders`.
   - `MINUTE_END`: `sync_last_sale_prices`; `metrics_tracker.end_of_bar` (reads the ledger).
   - `SESSION_START`: `process_dividends`, `handle_market_open`, splits (`data_portal.get_splits`, `process_splits`),
     `metrics_tracker.start_of_session`.
   - `BEFORE_TRADING_START`: `sync_last_sale_prices`.
   - `SESSION_END`: `sync_last_sale_prices`, `cleanup_expired_assets`, `metrics_tracker.end_of_session`.
3. controller to controllable, per strategy: `account_update(broker_state)` (fills, last sale prices), then
   `clock_update(ts, evt)`. The controllable runs its event (`session_start`, `before_trading_starts`, `bar` ->
   `handle_data` -> new orders, `session_end`, `minute_end`).
4. controllable to controller: `PlaceOrders(OrderBatch)` on every clock update (empty when nothing was ordered), which
   `StrategyManager.PlaceOrders` collects in `_new_orders` and which releases the barrier (`get_orders`).
5. controller, after the barrier: `market.place_orders(new_orders, manager)` -> `StrategyManager.place_orders` (blotter +
   ledger), then `strategy_manager.clear()`.

Float-summation-sensitive order (must not change): sync prices, all transactions, all commissions (BAR); ledger
revaluation (`Ledger._update_portfolio`, lazy: compounds `returns` and `pnl` at each read of `portfolio`) happens where the
metrics tracker reads the ledger.

## Steps

(filled in below as they are done)
