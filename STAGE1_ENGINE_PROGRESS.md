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

### Step 2 done: broker service (in process)

`pluto/broker_service/service.py` `BrokerService` is the single writer of the controller side book (portfolio,
positions, account, per calendar a `Ledger` and the controller's blotter copy). `StrategyManager` creates it
(`manager.broker`), keeps only what the gRPC servicer and the process barrier need, and still offers `add_data_portal`,
`add_ledger`, `add_blotter`, `add_strategy` (the tests build the run through them) which delegate. `Controller.__init__`
attaches the market (`broker.attach_market(market)`; the market is the matching venue: `MarketSimulation.get_transactions`
is unchanged and is called by `BrokerService.match`).

Interface (all calls per calendar name; the controller calls them where it called the old code, same order):
`set_time(cal, dt)` (market clock + blotter date), `sync_last_sale_prices(cal, prices, dt)`, `match(cal)`,
`start_session(cal, dt)` (dividends, ledger session start, splits), `end_session(cal, dt)` (expired assets),
`submit(orders)` (new orders and relayed cancellations; an order of a known id is rejected, nothing recorded),
reads `get_ledger`, `get_positions`, `get_open_orders`; `subscribe(callback)` for `events.StreamEvent`;
`set_strategy(id)` (one strategy per book: a different second id raises).
`StrategyManager` subscribes to turn `Fill` events into the `TransactionCommission` of the strategy's next broker state.
Ledger additions: `Ledger.mark_prices(prices, dt)` (the controller's price push), `process_dividends` takes the dividends
(the service reads the adjustment reader), `close_position` returns the transaction.
Removed as obsolete: `StrategyManager` ledger/blotter/portfolio/account/position tracker state and methods,
`place_orders`, `get_transactions` comment, `AbstractMarket.place_orders`, `MarketSimulation.place_orders`.
Left alone: the unused gRPC `CancelOrder`/`CancelAllOrdersForAsset` handlers of `StrategyManager` (they referenced a
non-existent `self._broker` before and still do), and the unused `pluto/finance/broker/broker_service.py`.
Seven files: 71 passed (baseline 71 passed).

### Step 3 done: deterministic order ids (CR7)

`SimulationBlotter.order` (the strategy's blotter, Pluto's own copy; zipline's blotter is untouched) mints
`<session_id><sequence as 8 hex digits at least>` when no id is given: `session_id` is the strategy's process id
(`md5(strategy_id + session_id)`, 32 hex digits: strategy and run in one), the sequence counts the orders the blotter placed
(an order of no shares takes no number). The id stays hexadecimal because `zipline.finance.order.Order.__hash__` is
`int(self.id, 16)`; 40 characters instead of the 64 of `uuid4().hex + session_id`.
Checked before changing: no test compares an id. `pluto/test_utils/test_utils.py` `_normalize` drops `id`, `order_id`,
`session_id` from the orders/transactions it compares with Zipline (whose ids stay random uuids: plain `TradingAlgorithm`,
untouched), and the scenario strategies only pass ids back to `cancel_order`. `tests/test_pluto_order_ids.py` (6 tests).
Open for the replay step: the sequence restarts at 1 when a blotter is rebuilt (a recovering runtime needs the counter in its
checkpoint, or ids that survive: the controller rejects an id it already knows as `OrderRejected`).
Seven files + order id tests: 77 passed.

### Step 4 done: per-strategy event stream, fold, saved sequence

Files: `pluto/broker_service/events.py` (events + `StreamEvent` envelope), `books.py` (`Books`: portfolio, positions, account and
one `Ledger` per calendar, shared by the service and the fold), `stream.py` (`InMemoryStream` sink, `Replica(Books)`, `fold`).

Stream: `StreamEvent(strategy_id, sequence, calendar_name, event)`; `sequence` is 1, 2, 3... without gaps per book
(`InMemoryStream` refuses a gap or a repeat); `strategy_id` is the process id given to `BrokerService.set_strategy` (one
strategy per book). Events, one per write to a ledger of the controller side: `OrderAccepted`, `OrderCancelled`,
`OrderExpired`, `OrderRejected` (duplicate order id; nothing recorded), `OrderTriggered`, `Fill`, `Commission`, `PricesMarked`,
`Dividends` (every session start, possibly empty: the due ones are paid), `SessionStarted`, `Split`, `Expiry`, `Revalued`.
Orders in events are snapshots (`copy.copy`).

Why `PricesMarked`, `SessionStarted`, `OrderTriggered`, `Revalued` exist (found while making the fold exact):
- `Ledger._update_portfolio` is lazy: every read of `portfolio` after a change compounds `returns` and `pnl`, so where the
  reads happened changes the last digits (`RevaluationTests` has prices where it does). `Ledger.on_revalue` (set by the
  service) publishes `Revalued` at each real recomputation; the fold calls `Ledger.revalue()` there. On the 28 examples and
  scenarios (daily frequency) dropping `Revalued` does not change the state (measured: all 28 still equal), only the unit test
  with three price steps shows it; minute frequency has a read per minute.
- the engine's ledger holds the blotter's live `Order` objects (so an order the market changes without filling, a stop or
  limit reached, shows in the ledger). The fold keeps one live order per id (`Replica.live_order`, updated in place by each
  event) to reproduce this, including `Ledger.orders(dt)` for earlier times; `OrderTriggered` carries such a change.

`fold(events, replica)` skips an event whose sequence is not above the `stream_sequence` of the ledger of its calendar: folding
a stream twice, or again after a restore, changes nothing (idempotent, at-least-once friendly). `Ledger.stream_sequence` is
written by the service (`_publish`) and the fold, saved by `get_state` (new field `stream_sequence = 10` in
`protos/ledger_state.proto`, regenerated with grpcio-tools like the previous commit) and restored by `restore_state`.
`STATE_VERSION` is now 2; version 1 states are still read (sequence 0), version 0 and above 2 are refused with the same message
shape (`format version N, expected 1 to 2`).

Test (`tests/test_pluto_broker_service.py`): for each of the 5 examples and 23 scenarios the run is made with a recording
service; the state serialised by `get_state` of the engine's ledger equals, byte for byte, the state of the folded ledger;
also `orders()`, `transactions()`, `orders(dt)`, the saved sequence, ids numbered 1..n after the process id, no rejection,
folding twice. Unit tests: duplicate, cancel, snapshots, sequence, gap, revaluation, trigger, state versions.
I also made the fold fail on purpose (skipping `PricesMarked`, `Commission`): the test fails, so it is not vacuous.
