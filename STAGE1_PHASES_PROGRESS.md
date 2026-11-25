# Stage 1, phases: the controller drives a run through phase events on the bus

Branch `stage1-phases` from `origin/master` `c4560706`. Worktree `/home/yves/.cache/hades-pluto-dev/stage1-phases/wt`, env
`.../stage1-phases/env`, scratch `.../stage1-phases/` (`t.sh` runs pytest from the worktree). Not pushed.
Gate: every test in `tests/` passes unchanged, exact tolerances; no result changes.

## Step 1: map of `Controller.clock_update` before the change

`Controller.clock_update(ts, evt, signals)`: `ts` is the global clock time of the tick, `evt` the global event, `signals` the
list of `(cts, c_evt, calendar)` of the calendars that have an event at this tick (`SimulationLoop.start`; one calendar,
XNYS, in every test). One tick runs these steps, in this order:

1. `strategy_manager.set_global_timestamp(ts)` (the stale-batch filter of `PlaceOrders`/`SetWatchList`).
2. `behavior.ingest_watchlist_data` (no-op in simulation).
3. `last_sale_prices = behavior.get_last_sale_prices(ts, manager, signals)`: **before any write of this tick**: reads
   `broker.get_positions(cal)` of the calendars whose signal is `BAR`, `MINUTE_END`, `TRADE_END`, `SESSION_END` or
   `BEFORE_TRADING_START`, and the price of each non-zero position from the data portal at `cts`; `{calendar: [(sid, price)]}`.
4. Per signal, in the order of `signals`, `calendar_name`:
   - always `broker.set_time(cal, cts)` (market clock, blotter date);
   - `BAR`, `TRADE_END`: `broker.sync_last_sale_prices(cal, prices[cal], cts)`, `broker.match(cal)` (the market's slippage and
     commission models on the calendar's `BarData`; all transactions are applied, then all commissions; every fill is a `Fill`
     stream event, which `StrategyManager._on_book_event` turns into a `TransactionCommission` of the strategy's next
     `BrokerState`); `blotter.prune_orders` is inside `match`;
   - `MINUTE_END`: `sync_last_sale_prices`, `broker.revalue(cal)`, **metrics tracker** `end_of_bar(cts, cal, ledger)`;
   - `SESSION_START`: `broker.start_session(cal, cts)` (dividends, `revalue`, ledger session start, splits),
     `broker.revalue(cal)`, **metrics tracker** `start_of_session(cts, cal, ledger)`;
   - `BEFORE_TRADING_START`: `sync_last_sale_prices`;
   - `SESSION_END`: `sync_last_sale_prices`, `broker.end_session(cal, cts)` (expired assets), `broker.revalue(cal)`,
     **metrics tracker** `end_of_session(cts, cal, ledger)`.
5. `values` = the prices of all calendars flattened (`AssetPrice(sid, price)`), then per strategy, in registration order:
   `broker_states[pid].last_sale_prices.extend(values)` and **to the controllable** `strategy.account_update(brs)`
   (`StrategyStub` -> `UpdateAccount` -> `AccountUpdate` command -> `Controllable.update_blotter_and_positions`: the fills and the
   last sale prices; the timestamp of the state is never set).
6. Per strategy, in registration order: **to the controllable** `strategy.clock_update(ts, evt)` (`StrategyStub` ->
   `ClockUpdate` -> command: the controllable's own clock decides which of `session_start`, `before_trading_starts`, `bar`
   (-> `handle_data`, new orders), `session_end`, `minute_end` it runs). The command ends with **to the controller**
   `broker.SetWatchList(WatchList)` then `broker.PlaceOrders(OrderBatch)` (the batch is empty when nothing was ordered, and
   also when the controllable has ended) through the in-memory `BrokerStub` straight into `StrategyManager.SetWatchList` /
   `PlaceOrders`, which keep the watchlist and convert the batch to `new_orders` (zipline orders, in the order placed), release
   a barrier (`_orders_complete`).
7. `new_orders = strategy_manager.get_orders()` (waits for that barrier), `behavior.ingest_all_data`,
   `behavior.update_watchlist` (no-ops), `broker.submit(new_orders)` (blotter + ledger + `OrderAccepted`/`OrderCancelled`...),
   `strategy_manager.clear()` (new orders, watchlist and the per-strategy `BrokerState` start again).

What crosses to the controllable: `account_update` (`BrokerState`: fills and last sale prices), `clock_update` (`ClockEvent`).
What comes back: `SetWatchList`, `PlaceOrders` (`WatchList`, `OrderBatch`; `ts` of the tick). What crosses to the metrics
tracker: the three calls of step 4 (the tracker is on the controller side and reads the ledger object of the book).

**Constraints that affect floats** (all kept by the change):
- the transactions of a bar, then its commissions, in the order the market produced them (inside `BrokerService.match`);
- `sync_last_sale_prices` before `match` on a bar; prices are read at the start of the tick, before the tick's writes;
- `broker.revalue(cal)` immediately before each metrics tracker read (`end_of_bar`, `start_of_session`, `end_of_session`):
  `Ledger` revalues lazily and compounds returns and pnl at each recomputation, so where it happens is part of the result;
- with two calendars the writes and the reads interleave per signal (signal 1 broker, signal 1 metrics, signal 2 broker...):
  the calendars' ledgers share one portfolio, so the phases of the signals cannot be batched;
- account update before clock update; the orders of a tick reach the book after every strategy ran, in registration order.

## Steps

(see below, one entry per step, added as they are done)
