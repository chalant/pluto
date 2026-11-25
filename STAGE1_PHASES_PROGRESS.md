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

### Step 2a done: book and metrics phases

`pluto/control/phases.py` (phase table, `PhaseDriver` = the controller's lock-step, `report`/`publish_done` for the participants,
`codec()`), `protos/phases.proto` (`SignalEvent`, `AccountEvent`, `PhaseDone`; generated with grpcio-tools like the others),
`pluto/control/controller/participants.py` (`BrokerParticipant`, `MetricsParticipant`). `StrategyManager(asset_finder, stp, bus=None)`
owns the bus of the run (`InMemoryBus` by default), `Controller` drives through `manager.bus`. The signals' writes go through
`phase.book` and the tracker reads through `phase.metrics`, per signal, in lock-step. Handlers catch their own failure and
report it in the `done` (the bus would redeliver a handler that raises: a phase is not idempotent); the controller raises
`PhaseError` for a missing done, a done with an error, a stale or duplicate done, or a dead letter. Examples + scenarios exact
(26 passed); `tests/test_pluto_phases.py`: 13 tests of the driver and the codec on a bare bus.

### Step 2b/3 done: account, trade and submit phases; orders, watchlist and broker state over the bus

`phase.account` (the broker side sends each strategy its `BrokerState` on `strategy.account.<process id>`: the fills of the tick
and the last sale prices; the runtime applies it), `phase.trade` (the runtime runs the clock event), `phase.submit` (the broker side
submits the orders of the tick and starts the next one). `pluto/control/processes/endpoint.py` `RuntimeEndpoint` is the runtime's
side (built by `StrategyManager.add_strategy`, the in-process wiring; the tests build the run through `manager.add_strategy`).
The in-memory `BrokerStub` of the controllable publishes the `OrderBatch` and `WatchList` on `strategy.orders` and
`strategy.watchlist` (key = process id) instead of calling the servicer; `StrategyManager.receive_orders` / `receive_watchlist` are
the shared bodies of the bus handlers and of the gRPC `PlaceOrders` / `SetWatchList`. The runtime's `done` of the trade phase is
published when its batch of orders is seen on the bus (publish only enqueues, so the ids cannot be known earlier): a runtime that sends
no batch leaves the phase without its done and the run aborts. Removed as obsolete: the order barrier (`get_orders`,
`_orders_complete`, `_order_counter`: the controller waits for the dones), `StrategyStub.clock_update` (now `clock_event(event)`:
the runtime runs the `ClockEvent` the controller published). Full suite exact.

### Step 4 done: runs over a serialising bus

`tests/test_pluto_phases_run.py` (patches the default bus of `StrategyManager` with a recording bus, plain or behind
`SerializingBus(..., phases.codec())`): `buy_and_hold`, `momentum_pipeline`, `cancel_orders` and `dividends` give the same frame
(every column, exact) over both, the same messages, only bytes on the inner bus, no dead letter. Every payload is a protobuf
message and is serialised: nothing is left out. Also: what a run publishes (counts per topic, content of the dones), and a failing
strategy aborts the run in the trade phase with the error in the `PhaseError`.

### Step 5 done: measurements (indicative: a shared 16-core machine, load average 12 to 17 during all runs)

Suite wall time, the 341 tests of `origin/master` (the new test files left out), alternating runs, same machine and env, each from
its own worktree: before 80.3 s and 79.3 s (pytest), after 98.7 s and 98.7 s: **about +23 %**, on every run. The cost is the bus, not
the work: a `buy_and_hold` run (7312 clock events) under cProfile goes from 5.4 s to 8.4 s; 93,276 messages published and 100,588
deliveries per run (about 11 microseconds per message in `publish` under the profiler, plus dispatch and the protobuf messages and
timestamp conversions around them). The dones are 42 % of the messages.

Messages per run of `buy_and_hold`, serialised (`SerializingBus`): 93,276 messages, 2.45 MB: `phase.done` 39,328 (1.33 MB), `phase.book`
5,040, `phase.metrics` 5,040, `phase.account` 7,312, `phase.trade` 7,312, `phase.submit` 7,312, `strategy.account.<id>` 7,312,
`strategy.orders` 7,312, `strategy.watchlist` 7,308. The metrics phase is published also for events that need no read (`BAR`,
`BEFORE_TRADING_START`: the participant says done with count 0): the controller does not know which events the tracker reads.

### Step 6 done: docs

`dev/README.md`: the run as phases on the bus (table, rules), the tree of what runs, the tests table with the bus, broker service,
order id and phase test files, the test command is `pytest tests`.

## Left open

- Cost of the bus (+23 % on the suite): fewer messages are possible (a metrics phase only for the events the tracker reads, one done
  for several participants), not done: it changes what the controller knows, a design choice.
- Signals of one tick run one after the other (book, metrics per signal), which keeps today's interleaving for several calendars
  (not exercised through the controller: every run in the suite has one calendar, XNYS); multi-calendar decision schedules need the
  phases per calendar to be rethought (stage 1 stays on today's calendar path).
- Only the in-process runtime that runs synchronously (`running_behavior.Blocking`) is supported; a `NonBlocking` runtime in process
  would publish from another thread (the in-memory bus is single-threaded) and its batch would come after its done [INFERENCE: not
  run]; a runtime that sent no batch when its handler returned aborts the run. Out-of-process runtimes need the networked bus
  (a later stage). Direct (non-bus) calls left: see the list of M1 under "Review fixes".
- The controller's behavior still reads `broker.get_positions` to price them, and the metrics tracker reads the ledger object of the
  book: two direct reads that a distributed deployment would have to replace (stream fold, price request).
- `StrategyManager.add_strategy` builds the runtime's endpoint (the in-process wiring); the registry of components of a distributed
  deployment is not designed here.
- The bus logs every message and never trims (open in `STAGE1_BUS_PROGRESS.md`): 93k messages for the 7312 clock events of `buy_and_hold`.
- A strategy exception is now a `PhaseError` carrying the error type and message (the traceback is in the log, `Participants` /
  `RuntimeEndpoint` loggers), no longer the original exception object.
- The bus is injected through `StrategyManager(..., bus=...)`; the tests build the manager through the unchanged test wiring, so
  the serialising-bus tests replace the default bus (`mock.patch` of `process_manager.InMemoryBus`).

## Review fixes

Before: 360 tests passed (341 of `origin/master` and 19 of this branch), 116 s. Each fix is one commit.

- **H1 (order of the orders with several strategies)**: confirmed. A runtime's batch has `producer_id` = its process id, so with two
  strategies the batches arrived by process id, not in registration order. `StrategyManager` now keeps the received batches by
  process id (`_received_orders`) and `new_orders` builds the list in registration order (`_processes`) when the broker side
  submits. Test `tests/test_pluto_phase_participants.py`: three fake runtimes registered as `zzz`, `aaa`, `mmm` (ids on both sides of
  `'controller'`), each ordering the same asset in one tick; the submitted order is `zzz, aaa, mmm` (fails without the fix: `aaa, mmm, zzz`).
- **M2 (the runtime's trade `done`)**: confirmed (N+1 deliveries per batch). `RuntimeEndpoint` no longer subscribes to
  `strategy.orders`: it publishes the trade `done` at the end of its trade handler, after the clock event ran, with the number of
  orders it sent. It counts them because the runtime's orders and watchlist now go out through `RuntimeEndpoint.send` (the in-memory
  `BrokerStub` reaches it with `StrategyManager.runtime(process id)`). A runtime that sent no batch says so in its done (error,
  aborts the run; the controllable always sends one, possibly empty). In the submit phase `BrokerParticipant` (which listens to the
  trade dones) compares, per process, the number of orders in the done with `StrategyManager.received_orders` (batches accepted for
  the tick): a lost batch or a stale one (ignored by the manager) is a `PhaseError` before anything is submitted. Tests: no batch,
  lost batch, stale batch (`tests/test_pluto_phase_participants.py`); `test_what_a_run_publishes` asserts counts in the dones and
  compares the orders of the batches with the fills the strategy was told (the ids of the removed done content).
- **M3 (unused `PhaseDone` content)**: confirmed: nobody read `ids`, `sequence`, nor `count` except the trade done after M2.
  `protos/phases.proto` `PhaseDone` is now `phase`, `participant`, `orders` (trade phase: orders the runtime sent) and `error`;
  `protos/phases_pb2.py` regenerated with `python -m grpc_tools.protoc -I. --python_out=. protos/phases.proto`. `Summary` is gone;
  `publish_done(bus, phase, ts, participant, orders=0, error='')`; the participants' work returns nothing; `BrokerParticipant`
  no longer follows the book's stream (it only did so to fill the dones); `PhaseDriver.drive` returns nothing (the controller
  never read what it returned).
- **M1 (out-of-process and `NonBlocking` runtimes)**: confirmed: with the bus only the in-process `Blocking` runtime works, the other
  cases ended in a "no done" abort. `StrategyManager.PlaceOrders`, `SetWatchList` and `Stopped` (gRPC) raise `NotImplementedError`
  saying that out-of-process runtimes need the networked bus; `receive_orders` / `receive_watchlist` are the bodies of the bus
  handlers only. Deleted (nothing read them, checked with grep): `_active_processes` (and its `Stopped` decrement), `_watchlist_counter`,
  `_watchlist_complete`, both locks (the bus handlers run on one thread; they guarded the gRPC threads of the barrier),
  `get_watchlist` (it waited for the barrier: replaced by the plain `watchlist` property, the assets asked for in the tick; no
  caller yet, the behavior hook `update_watchlist` is where it belongs) and the session argument of `receive_watchlist`.
  `dev/README.md` states stage 1 supports only the `Blocking` runtime and lists the direct (non-bus) calls: positions read for
  pricing, the metrics tracker reading the ledger object, the controller reading `session_ids`, behavior hooks receiving the manager,
  the broker stub reaching into the servicer (cancel stubs, the runtime's endpoint), `add_strategy` building the endpoint.
  `session_ids` keeps its name: `service_access.per_session` reads it too.
- **Missing tests** (`tests/test_pluto_phase_participants.py`, all with a fake book and a fake tracker that record their calls in one
  list, fast): the book and metrics participants over the six clock event types (calls and their order, `revalue` right before
  each tracker read); a tick with two signals (two calendars): broker, metrics, broker, metrics; the stale-batch filter on the bus
  path (a batch and a watchlist of another clock event are ignored); a failure in the account phase and one in the submit phase
  abort with `PhaseError`.
- **Low items**: comment at the `set_global_timestamp` call (`BrokerParticipant._account`): it feeds the stale-batch filter of the
  manager; the policy that a done from an unexpected participant is accepted is in the docstring of `phases` and of
  `PhaseDriver.drive`, and tested; type hints added: `report(work: Callable[[], None])`, `drive(...)` (payload `Message`, `expected`
  `Iterable[str]`, returns `None`), the delivery handlers (`Delivery`), `_on_book_event(StreamEvent)`, and the `metrics_tracker`
  argument (`participants.MetricsTracker`, a `Protocol` of the three reads the participant makes; the tracker of the tests is duck
  typed). `session_ids` is not renamed: `service_access.per_session` reads it too.

### After the review fixes

`buy_and_hold` (7,312 clock events, one strategy), plain `InMemoryBus`, measured by counting the calls of the handlers (the same as the
return of `run_until_idle` summed): 93,276 messages and **100,588 deliveries per run at `139ed2f7`** (the branch before the review
fixes); **132,604 at `eb5d89ff`, the first version of the fixes** (`BrokerParticipant` subscribed to every `phase.done`: +39,328 for it, -7,312 for the runtime's
own `strategy.orders` subscription that M2 dropped: the reviewer was right, deliveries went up by 32,016); **100,588 after the
per-phase done topics** (the broker side only gets the 7,312 trade dones: back to the number of `139ed2f7`, 7,312 fewer for the
runtime and 7,312 more for the broker side). The 132,604 of step 5 above was not reproduced for `139ed2f7` (it is the number of `eb5d89ff`);
messages are 93,276 in all three states (2.38 MB; the dones are 39,328 of them), the fixes change who hears a done, not how many are
published.

Done topics: `phase.done.<phase>` (`Phase.done_topic`), the controller's `PhaseDriver` subscribes to `phase.done.>`, `BrokerParticipant` only
to `phase.done.trade`.

### Left open (protocol changes, not done in the review round)

Counts measured on the same run (`buy_and_hold`, 7,312 clock events, one strategy, 5,040 signals); no time measured:

- **Metrics phase only for the events the tracker reads** (`MINUTE_END`, `SESSION_START`, `SESSION_END`): of the 5,040 metrics phases
  2,518 (`BEFORE_TRADING_START`, `TRADE_END`) read nothing: 2,518 messages and 2,518 dones, **5,036 fewer messages (5.4 %)**. The
  controller would have to know which events the tracker reads.
- **Watchlist folded into the order batch**: 7,308 `strategy.watchlist` messages (275 kB, 1,258 of them non-empty), 7,308 fewer
  messages (7.8 %), the manager would take the watchlist from the batch it checks anyway.
- **Merging phases** (one done for several participants, or the metrics reads inside the book phase): every phase costs its
  event plus one done per participant; the metrics phase is 10,080 messages (10.8 %) with its dones, and the dones are 42 % of all messages.
  Merging changes where the lock-step is checked and what the controller knows: a design choice.
- Bus log trimming and retention (open in `STAGE1_BUS_PROGRESS.md`).
