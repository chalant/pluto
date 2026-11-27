# Stage 2, fault bus (branch `stage2-faultbus`)

Base: `origin/master` = `dc3e9aa4`. Worktree `/home/yves/.cache/hades-pluto-dev/stage2-faultbus/wt`, env `.../stage2-faultbus/env`
(`dev/setup_env.sh`), scratch `.../stage2-faultbus/scratch`. Task: `SeededFaultBus`, a seeded fault-injecting wrapper over any
`Bus` (Execution Topology ET5 and ET8), so that the failure paths that only exist distributed are tested in one process,
reproducibly. A new module and new tests only: no existing module (`pluto/control/`, `pluto/bus/*`) touched, no new dependency.

## Status

- [x] fault model (below, and the docstring of `pluto/bus/faults.py`)
- [x] `pluto/bus/faults.py`: `SeededFaultBus(inner, seed, faults)`, `FaultConfig`, `Fault`, `FaultKind`, `PublishFailed`
- [x] `tests/test_bus_faults.py` (transparent over the whole contract; determinism; FIFO; one test per fault)
- [x] `tests/test_bus_faults_contract.py` (the contract suite under faults: which tests fail, as a checked list)
- [x] protocol analysis (below), checked by a scratch run of whole backtests over the fault bus (not committed)
- [ ] final run from a fresh clone (see "Final run")

Not exported from `pluto/bus/__init__.py` (an existing module: import from `pluto.bus.faults`). Its docstring still says
"Not here: ... seeded fault injection": a one-line follow-up for whoever merges.

## Fault model

The bus has no clock (S5); the wrapper's time is the number of handler invocations so far (a "tick"). The spike's finding that
decides what is legal: the networked bus is an ordered log with a client-side dispatcher, so S3 holds among the messages that
have *arrived*, and across processes only after barriers; S1 (FIFO per lane) is all that holds across arrivals.

| Fault (`FaultConfig`) | Allowed because | How the wrapper produces it | What a correct consumer does |
|---|---|---|---|
| duplicate delivery (`ack_lost`) | S7 at-least-once; an ack can be lost | after the handler returned, `nack` instead of ack: the inner bus redelivers the same message (`attempt` + 1) before anything later of its lane | idempotent on `msg_id`; what it publishes while handling has ids derived from its input |
| late duplicate (`duplicate_publish`) | S7: no dedup, publishers retry | a second copy (same envelope) is published 1 to `max_delay` ticks later: after later messages of its lane, to every subscriber, `attempt` 1 | idempotent on `msg_id`, also for a copy that arrives after the protocol moved on |
| delayed arrival (`delay`, `max_delay`) | S1: nothing across lanes; S3 orders what has arrived | a publish is held 1 to `max_delay` ticks, every later publish of the same (topic, key) waits behind it (FIFO kept); the inner bus dispatches by S3 what it has | never infer from a dispatched message that an earlier-sorting one does not exist; wait for explicit evidence (a count, a done, a deadline), not "idle" |
| lost delivery (`lose_delivery`) | S7: never acked, so redelivered | `nack` instead of calling the handler; counts as an attempt; after the inner `max_attempts` a dead letter | expect `attempt` > 1 on first sight; a dead letter is a failure to report |
| consumer crash and resume (`crash`, `max_crashes`) | S9: resume at the stored `position_of` | cancel the inner subscription, subscribe again at the position of the last settled delivery, before the handler ran or after it ran and before the ack (1 in 2). Exact, ungrouped topics only (a position is per topic) | checkpoint with `position_of`; everything above the checkpoint comes again, handled or not: idempotent, or rebuild state from the checkpoint. The subscription id changes |
| publish failure (`publish_fail`) | a client may see an error with nothing sent | `PublishFailed` before the inner bus saw the message (nothing logged, no `seq` used) | retry with the same `msg_id` |
| ambiguous publish (`publish_ack_lost`) | the ack was lost, the publisher cannot know | published normally (maybe delayed), then `PublishFailed` | retry with the same `msg_id`: a duplicate to every consumer |
| silent participant (`mute`, `SeededFaultBus.mute/unmute`) | a hung or dead process | a subscription swallows deliveries (acknowledged, handler never called) | a waiter needs a deadline; no answer is a failure, not a state |

Contract-violating, only with `FaultConfig(contract_violating=True)`: `reorder_within_lane` (a message jumps ahead of the held
one before it in its lane: S1 broken). The FIFO check of the tests must fail under it (tested).

Not modelled: silent loss of an accepted message, corruption, per-subscriber lag (all subscribers see a message arrive at the
same tick), loss of a checkpoint, partitions. Rules of the wrapper: a delayed publish's `Receipt.position` is a token resolved when
the message reaches the log and `subscribe(start=...)` accepts it; `subscribe` is a barrier (what is held is logged first, so
`Start.LATEST` still skips what was published before); `step`/`run_until_idle` skip ahead in time when the inner bus is idle, so
held messages are always delivered and "idle" means nothing pending anywhere; the wrapper owns `seq` (per producer, from 1, like
the in-memory bus); injected redeliveries are capped by `max_redeliveries` per message and subscription (keep the inner
`max_attempts` above it); a retried publish is a new call: same `msg_id` (explicit), next `seq`. Every decision comes from one
`random.Random(seed)`, a fixed number of draws per publish and per delivery, so a seed's stream does not depend on which faults
were hit.

## Evidence

Tests: `tests/test_bus_faults.py` runs on `InMemoryBus` and `SerializingBus(InMemoryBus)`: the whole broker-neutral contract
(84 tests) passes over the wrapper with no faults, and the delivery trace of the contract scenario equals the unwrapped one; same
seed gives the same fault scenario trace, also under `PYTHONHASHSEED` 1 to 5 (subprocesses); different seeds give different traces
and fault logs; per-lane FIFO holds on 25 seeds with every allowed fault on, and the same check fails under the contract-violating
fault; duplicates have the same `msg_id`; one focused test per fault.

### The contract suite under faults (`tests/test_bus_faults_contract.py`, `python -m tests.test_bus_faults_contract`)

Publish failures are left out (the contract tests do not retry, so every test that publishes fails on `PublishFailed`: 82 of 84
fail with `publish_fail`, which only shows that a publisher must retry). For each other fault, on both implementations and seeds
0 to 2 (fault probability 0.5 per decision):

| Fault | Contract tests that fail | Why |
|---|---|---|
| `delay` | **8**: `test_dispatch_picks_lowest_logical_ts_then_producer_then_seq`, `test_dispatch_is_across_topics_too`, `test_dispatch_does_not_depend_on_publish_order_across_lanes`, `test_phase_orders_messages_of_the_same_logical_ts`, `test_equal_content_falls_back_to_topic_then_key`, `test_wildcards`, `test_a_replay_delivers_in_the_order_a_live_subscriber_saw`, `test_position_of_is_a_resume_point_without_gaps_when_keys_complete_out_of_log_order` | exactly the ones asserting the exact global S3 order across lanes; including that a replay equals what a live subscriber saw (replay merges the whole log by S3, live dispatch saw only what had arrived). No other test fails: everything else in the contract survives a delay |
| `lose_delivery` | 14: the attempt counters (`test_nack_redelivers_before_the_next_message_of_the_lane`, `test_dead_letter_after_max_attempts_and_the_lane_moves_on`, `test_exception_is_a_nack_and_a_redelivery`, `test_ack_after_nack_wins_and_an_exception_always_nacks`, `test_latest_skips_the_log_and_gets_new_messages`, `test_handler_rank_orders_the_handlers_of_a_message`, `test_wildcards`...) and invocation counts (`test_run_until_idle_drains_cascades_and_counts_invocations`, `test_subscribing_never_delivers_inline`, `test_max_deliveries_equal_to_the_work_is_not_an_error`: a lost delivery is an invocation); group and cancel tests that count them | the handler still sees each message once, but `attempt` is not 1 and the bus ran more invocations. `test_each_subscription_sees_the_whole_stream_once` still passes |
| `ack_lost` | 52 | exactly-once delivery: `test_each_subscription_sees_the_whole_stream_once`, `test_fifo_per_topic_and_key` (93 != 60), `test_the_bus_does_not_deduplicate`, replay, resume and position tests (a message handled before the checkpoint comes again), every exact-sequence assertion, plus attempts |
| `duplicate_publish` | 54 | the same, from a copy in the log (replay of the log shows it too) |
| `crash` | 27 | exactly-once on exact topics (`test_each_subscription_sees_the_whole_stream_once`, replay, resume, position tests); `test_position_of_is_a_resume_point_without_gaps...` raises `BusError`: a stored `Delivery` of a subscription that restarted is dead, only stored positions survive; pattern and group tests are unaffected (they do not crash) |

27 tests pass under every one of these faults (request and reply, close and cancel, argument checks, envelopes, repeatability:
the list `SURVIVORS` of the module). `test_delivery_trace_is_repeatable` passing under all faults is the determinism evidence.
Those are the guarantees to rely on; the others (exactly-once, S3 order across lanes, `attempt` = 1, replay = live) the phase
protocol **must not rely on**. The checks in the module: delay fails exactly the 8 order tests; duplicates fail the exactly-once
tests; a lost delivery fails the attempt tests but not the exactly-once ones; the survivors never fail.

## Where the current phase protocol breaks under each fault

From reading `pluto/control/phases.py`, `controller/participants.py`, `controller/controller.py`, `controller/process_manager.py`
and `processes/endpoint.py` (master `dc3e9aa4`), then checked by running whole `buy_and_hold` backtests (the 2008 example, a
strategy, serialising wrapper inside the fault bus) with one fault at a time (probability 0.1, 3 seeds; scratch script, not committed;
`delay` 0.3). Results: `ack_lost`, `duplicate_publish`, `crash`, `mute`, `publish_ack_lost`, `publish_fail`: every run aborted with a
`PhaseError` (or a raw `PublishFailed`) within the first ticks; `delay` (22,000 delayed messages) and `lose_delivery` (8,300 lost
deliveries): results identical to the run without faults.

Why the last two pass: `PhaseDriver.drive` ends in `run_until_idle`, and the wrapper makes "idle" mean nothing is pending *anywhere*,
so a delay never outlives a phase; a lost delivery is redelivered inside the same phase. That is a property of the in-process
driver, not of the protocol: the other worker's driver waits for the expected dones, and over a real network "idle" cannot be
observed. The analysis below is for that driver.

**What every handler lacks: idempotence.** Each phase handler does its work, then publishes a done; none remembers it handled the
event. And the ids do not allow a consumer to dedupe: `publish_done`, `RuntimeEndpoint.send_orders`, `BrokerParticipant._account`
(account state) and `PhaseDriver.drive` publish without `msg_id`/`seq`, so every handling of an event publishes under a *new*
`producer:seq`: a repeated handling does not repeat the same message, it makes a new one. Requirement: deterministic `msg_id`s at
every publish site, derived from what is being answered, e.g. `done:<phase>:<ts>:<participant>`, `orders:<process>:<ts>`,
`account:<process>:<ts>`, `phase:<phase>:<ts>:<key>`; and in each participant a record of handled phase events (by `msg_id`) so that
a duplicate event re-publishes the same done (or nothing) instead of working again.

| Fault | Where it breaks | What the protocol needs |
|---|---|---|
| **duplicated done** (`ack_lost` on the driver's subscription, `duplicate_publish`, `publish_ack_lost` retry, a restarted participant) | `PhaseDriver.drive`: `'<p> said done twice'` for the same participant (seen in every run); a late copy after the phase ended arrives during the *next* `drive`, which `dones.clear()`ed, and is reported as `'sent a done of <phase> at <ts>'` (wrong phase or tick: also an error) | ignore a done whose `msg_id` (or (phase, ts, participant)) was already accepted or whose tick is past; count distinct participants. Errors only for a *different* done of the same participant and phase (conflicting results) |
| **duplicated phase event** (driver's publish retried after `publish_ack_lost`; redelivery to a participant: `ack_lost`, `crash`) | `BrokerParticipant._on_book` runs `set_time`, `sync_last_sale_prices`, `match`, `revalue`, `observe` twice: the ledger compounds returns at each recomputation, so a second `revalue` or `observe` changes results (see the notes of `STAGE1_PHASES_PROGRESS.md`: where `revalue` happens is part of the result). `_on_account`: `state.last_sale_prices.extend(...)` twice (the state has every price twice) and the state is published twice, so the runtime applies the fills twice (`RuntimeEndpoint._on_account` -> `account_update`). `RuntimeEndpoint._on_trade`: runs the strategy's `clock_event` twice: `handle_data` twice, two batches of orders (seen: `'sent 0 orders in 1 batches, the broker received 0 in 2'`). `_on_submit`: `_check_orders` then `broker.submit(orders)` and `manager.clear()`; the second run submits nothing (cleared) so it is benign except for a second done | each participant keeps the ids of the events it handled this tick and answers a duplicate with the stored result only; the book/account/trade work is never repeated. The phase event carries the tick, so "handled (phase, ts, key)" is the idempotency key |
| **duplicated order batch** | `StrategyManager.receive_orders` extends `_received_orders` and counts the batch: two batches double the orders (two orders each with its own id) and `received_batches` 2 != 1 | `_check_orders` *detects* it (`sent n in 1 batches, received 2`: it caught every duplicate in the runs) but a detected duplicate aborts the run. Tolerance: dedupe in `receive_orders` by batch `msg_id` (`orders:<process>:<ts>`, deterministic), keep the first |
| **delayed order batch** (arrives after the runtime's trade done: different lane, `strategy.orders` vs `phase.done.trade`) | with a driver that waits for the expected dones, the submit phase starts when the trade dones are in; the batch may not have arrived: `_check_orders` fails (`received 0 in 0`); the manager's stale filter (`conversions.to_datetime(batch.ts) == self._global_ts`) silently drops a batch that arrives after the next tick's `set_global_timestamp` | explicit wait in the submit phase: the done states the count of orders and batches (it already does: `orders`, `batches`), so the broker waits until it received that many (or the driver does, before it publishes submit), with a deadline. A batch for a past tick is dropped by design but must be *counted* as a late arrival, not silently |
| **delayed account state** (`strategy.account.<pid>` vs `phase.trade`) | the runtime would run the clock event on a stale account if the trade event reached it first. Today the driver publishes `phase.trade` only after the account dones, and a runtime's account done is published after it applied the state, so the order holds by causality, not by S3. It breaks if a participant acts on `phase.trade` without that barrier (e.g. a driver that pipelines phases, or a restarted runtime that replays `phase.trade` before the account state of the tick) | the barrier must stay explicit: the runtime refuses `phase.trade` for tick T until it applied the account state of T (state carries its tick; the trade event waits or is nacked and redelivered) |
| **delayed done** | `PhaseDriver`: harmless if it waits for the expected dones; `BrokerParticipant._on_done` (trade dones, to learn the counts) is a *second* subscription to the same messages: with per-subscriber lag (not modelled by this wrapper) the broker may learn the counts after the driver published submit, so `_sent` is empty and `_check_orders` reports a spurious mismatch. A late duplicate trade done of the previous tick overwrites `_sent[participant]` with the old counts | the counts belong in the submit event (the driver, which has the accepted dones, puts them in the payload) or `_sent` is keyed by (participant, ts); never learn tick-bound state from a second stream of the same messages |
| **lost delivery** | redelivered by the bus; handlers that raise are redelivered too, so a phase whose handler fails *after* its work ran (e.g. `publish_done` raising inside `report`'s `else`) redoes the work. `report` only catches failures of `work()`; a `PublishFailed` from `publish_done` escapes, nacks the delivery and the whole phase work runs again | idempotence as above; publish-with-retry separated from the work; `attempt` > 1 is normal |
| **consumer crash and resume** | participants keep their state in memory and never call `position_of`: a restarted one starts from its checkpoint with no state, re-reads events it handled (duplicates as above) and has lost the tick's partial work; the strategy runtime's algorithm state is not rebuilt. Also the restarted subscription has a new id and a new registration order (S3 tie-break by registration) | at most: abort the run on a restart (a missing done), or checkpoint at tick boundaries (`position_of` of the last submit done) with the runtime state saved with it. Decide per participant; whole-backtest tests under `crash` should expect a clean `PhaseError` until that exists |
| **publish failure** | `drive` publishes the phase event with no retry: a raw `PublishFailed` escapes the controller (seen); `_account` publishing the state fails inside `work()`, is reported as a failed done (`'broker failed: PublishFailed'`) *and* then `fail`'s own `publish_done` can fail too; the runtime then never learns the state. After an ambiguous failure the retry is a duplicate of the event (above) | a bounded retry helper at every publish site that reuses the deterministic `msg_id`; a `PhaseError` only after the budget |
| **participant that never answers** | today `drive` reports `'no done from X'` right after `run_until_idle` (seen with `mute`): this works only because in-process quiescence is observable. A driver that waits for dones needs a deadline, and the bus has no clock (S5) | a deadline expressed in the protocol: the driver publishes a timeout message (a timer is a message) at `logical_ts` + d, or bounds the number of `step`s; on expiry it re-publishes the phase event with the same `msg_id` (safe once handlers are idempotent) up to N times, then raises `PhaseError` naming the missing participants |

The mismatch counts (`'sent n orders in m batches, the broker received ...'`) are the one defence the protocol has today: every
duplicate or lost batch of the runs was caught by it, none went unnoticed. The silent risks are the ones without a count: the
ledger (`revalue`/`observe` twice), the account state applied twice, and the stale account (delayed state).

Order of work for the next step (whole backtests over the fault bus, after the driver change merges): deterministic `msg_id`s at
all publish sites; a per-tick idempotence record in the three participants and the driver; `receive_orders` dedupe; counts in the
submit event; a retrying publish helper; then run `delay`, `lose_delivery`, `ack_lost`, `duplicate_publish` (results must equal
the run without faults) and `mute`, `crash`, `publish_fail` (clean `PhaseError`, or equal results once retries exist).
