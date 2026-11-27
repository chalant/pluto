# Stage 2, fault bus (branch `stage2-faultbus`)

Base: `origin/master` = `dc3e9aa4`. Worktree `/home/yves/.cache/hades-pluto-dev/stage2-faultbus/wt`, env `.../stage2-faultbus/env`
(`dev/setup_env.sh`), scratch `.../stage2-faultbus/scratch`. Task: `SeededFaultBus`, a seeded fault-injecting wrapper over any
`Bus` (Execution Topology ET5 and ET8), so that the failure paths that only exist distributed are tested in one process,
reproducibly. A new module and new tests; the only existing file touched is `pluto/bus/__init__.py` (docstring line and exports); no
`pluto/control/` change, no new dependency.

## Status

- [x] fault model (below, and the docstring of `pluto/bus/faults.py`)
- [x] `pluto/bus/faults.py`: `SeededFaultBus(inner, seed, faults)`, `FaultConfig`, `Fault`, `FaultKind`, `PublishFailed`
- [x] `tests/test_bus_faults.py` (transparent over the whole contract; determinism; FIFO; one test per fault)
- [x] `tests/test_bus_faults_contract.py` (the contract suite under faults: which tests fail, as a checked list)
- [x] protocol analysis (below), checked by a scratch run of whole backtests over the fault bus (not committed)
- [x] final run from a fresh clone (see "Final run")

Exported from `pluto/bus/__init__.py` (`SeededFaultBus`, `FaultConfig`, `Fault`, `FaultKind`, `PublishFailed`); its stale "Not here: seeded
fault injection" line was fixed (the only change to an existing module, besides the new import).

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
| consumer crash and resume (`crash`, `max_crashes`, `SeededFaultBus.crash`) | S9: resume at the stored `position_of` | subscribe the replacement at the position of the last settled delivery (before any: the first message logged after the subscription), then cancel the old subscription; before the handler ran or after it ran and before the ack (1 in 2). If the replay does not fit in the inner bus (`QueueFull`) the crash is skipped and logged (`crash_skipped`). Exact, ungrouped topics only (a position is per topic) | checkpoint with `position_of`; everything above the checkpoint comes again, handled or not: idempotent, or rebuild state from the checkpoint. The subscription id changes |
| publish failure (`publish_fail`) | a client may see an error with nothing sent | `PublishFailed` after the arguments were checked, before the inner bus saw the message (nothing logged, no `seq` used) | retry with the same `msg_id` |
| ambiguous publish (`publish_ack_lost`) | the ack was lost, the publisher cannot know | published normally (maybe delayed), then `PublishFailed` | retry with the same `msg_id`: a duplicate to every consumer |
| silent participant (`mute`, `SeededFaultBus.mute/unmute`) | a hung or dead process never acknowledges | the subscription is cancelled with the delivery in hand unsettled (nothing reaches the handler, nothing is acked, what is published meanwhile waits in the log); `unmute` restarts it at its checkpoint like a crash (exact, ungrouped topics: it reads again what it had not settled); on a pattern or group member a mute is permanent and `unmute` raises | a waiter needs a deadline; no answer is a failure, not a state |

Contract-violating, only with `FaultConfig(contract_violating=True)`: `reorder_within_lane` (a message jumps ahead of the held
one before it in its lane: S1 broken). The FIFO check of the tests must fail under it (tested).

Not modelled: silent loss of an accepted message, corruption, per-subscriber lag (all subscribers see a message arrive at the
same tick), loss of a checkpoint, partitions, **late redelivery** (after other lanes moved on, as after an ack timeout) and
**group-member failover**; see "What this evidence does not cover" below for what that means for `lose_delivery`.

Rules of the wrapper: every publish draws 7 and every delivery 5 random numbers whatever the configuration (the amounts are used
only when a fault is on), so two configurations that differ in one probability take the same stream (tested: equal RNG state after
N publishes and N deliveries); a delayed publish's `Receipt.position` is a token resolved when the message reaches the log and
`subscribe(start=...)` accepts it (`ValueError` if the message was dropped on release); `subscribe` is a barrier: what is held is
logged first, so `Start.LATEST` still skips what was published before. **Every `Request` subscribes to its reply topic, so every
request flushes all held messages** (the delays of earlier publishes end there), and with the inner bus full the barrier can raise
`QueueFull` where the bare bus would not; `step`/`run_until_idle` skip ahead in time when the inner bus is idle, so held messages are
always delivered and "idle" means nothing pending anywhere; the wrapper owns `seq` (per producer, from 1, like the in-memory bus);
injected redeliveries are capped by `max_redeliveries` per message and subscription, also across crashes (keep the inner
`max_attempts` above it); a retried publish is a new call: same `msg_id` (explicit), next `seq`; a held message the inner bus
refuses (other than `QueueFull`, which makes it wait) is dropped, logged as `release_failed` and its error raised from the step that
released it; `publish_fail` checks the arguments first, so bad arguments are never reported as an injected failure.

## Evidence

Tests: `tests/test_bus_faults.py` runs on `InMemoryBus` and `SerializingBus(InMemoryBus)`: the whole broker-neutral contract
(84 tests) passes over the wrapper with no faults, and the delivery trace of the contract scenario equals the unwrapped one; same
seed gives the same fault scenario trace, also under `PYTHONHASHSEED` 1 to 5 (subprocesses); different seeds give different traces
and fault logs; per-lane FIFO holds on 25 seeds with every allowed fault on, and the same check fails under the contract-violating
fault; duplicates have the same `msg_id`; one focused test per fault.

### The contract suite under faults (`tests/test_bus_faults_contract.py`, `python -m tests.test_bus_faults_contract`)

Publish failures are left out (the contract tests do not retry, so every test that publishes fails on `PublishFailed`: 82 of 84
fail with `publish_fail`, which only shows that a publisher must retry). Every other fault is on at **probability 1**, on both
implementations; the hash-seed test is left out (83 tests). A delay still draws its length from the seed, so its failing set is the
union over seeds: the first version of the check used probability 0.5 and seeds 0 to 2, which was seed luck (it missed one order test
and put two attempt tests among the survivors, and the counts were wrong). Measured now, 3 seeds and 20 seeds give the same unions:

| Fault | Contract tests that fail | Why |
|---|---|---|
| `delay` | **9** (3 on every seed, 9 over the seeds): `test_a_replay_delivers_in_the_order_a_live_subscriber_saw`, `test_dispatch_does_not_depend_on_publish_order_across_lanes`, `test_replay_then_live_messages_follow_the_dispatch_rule`, `test_dispatch_picks_lowest_logical_ts_then_producer_then_seq`, `test_dispatch_is_across_topics_too`, `test_phase_orders_messages_of_the_same_logical_ts`, `test_equal_content_falls_back_to_topic_then_key`, `test_wildcards`, `test_position_of_is_a_resume_point_without_gaps_when_keys_complete_out_of_log_order` | exactly the ones asserting the exact global S3 order across lanes, including that a replay (of the whole log, merged by S3) equals what a live subscriber saw (it saw what had arrived). Whether a given order test fails depends on the delays drawn; the checked claim is `ORDER` is in the union and `FIFO_PRESERVED` (per-lane FIFO, nack redelivered before the next message of the lane, `test_each_subscription_sees_the_whole_stream_once`, dead letters, no dedup) never fails |
| `lose_delivery` | **21**: attempt counters (`test_nack_redelivers_before_the_next_message_of_the_lane`, `test_dead_letter_after_max_attempts_and_the_lane_moves_on`, `test_exception_is_a_nack_and_a_redelivery`, `test_ack_after_nack_wins_and_an_exception_always_nacks`, `test_latest_skips_the_log_and_gets_new_messages`, `test_handler_rank_orders_the_handlers_of_a_message`, `test_wildcards`, `test_replay_leaves_existing_subscriptions_alone`), the two that assert a first attempt (`test_replay_redelivers_with_attempt_one_and_the_original_envelope`, `test_a_closed_bus_refuses_every_call`), invocation counts (`test_run_until_idle_drains_cascades_and_counts_invocations`, `test_subscribing_never_delivers_inline`, `test_step_runs_exactly_one_handler`, `test_publish_never_runs_a_handler`, `test_max_deliveries_equal_to_the_work_is_not_an_error`, `test_max_deliveries_zero_and_invalid_values`: a lost delivery is an invocation), group, cancel, close and exit tests that count them (`test_a_group_member_that_nacks_and_cancels_hands_the_retry_to_the_others`, `test_cancelling_a_group_member_hands_its_keys_to_the_others`, `test_a_handler_that_closes_the_bus_leaves_nothing_behind`, `test_an_exit_request_propagates_and_leaves_the_message_pending`) and `test_position_of_is_a_resume_point_without_gaps_when_keys_complete_out_of_log_order` | the handler still sees each message once, but `attempt` is not 1 and the bus ran more invocations. The exactly-once tests (`test_each_subscription_sees_the_whole_stream_once` and the rest of `EXACTLY_ONCE`) still pass |
| `ack_lost` | **52** | exactly-once delivery: `test_each_subscription_sees_the_whole_stream_once`, `test_fifo_per_topic_and_key` (93 != 60), `test_the_bus_does_not_deduplicate`, replay, resume and position tests (a message handled before the checkpoint comes again), every exact-sequence assertion, plus the attempt counters |
| `duplicate_publish` | **54** | the same, from a copy in the log (a replay of the log shows it too) |
| `crash` | **44** (42 on every seed) | exactly-once on exact topics (`test_each_subscription_sees_the_whole_stream_once`, replay, resume, position tests) and everything that holds a `Delivery` or a subscription id across a restart (`test_position_of_is_a_resume_point_without_gaps...` raises `BusError`: a stored `Delivery` of a restarted subscription is dead, only stored positions survive); `test_fifo_per_topic_and_key` (a pattern subscription) is unaffected |

**25** tests pass under every one of these faults (request and reply, close and cancel, argument checks, envelopes, repeatability:
the set `SURVIVORS` of the module). `test_delivery_trace_is_repeatable` passing under all faults is the determinism evidence.
Those are the guarantees to rely on; the others (exactly-once, S3 order across lanes, `attempt` = 1, replay = live) the phase
protocol **must not rely on**. The checks in the module: `ORDER` (9) is in the failures of a delay and `FIFO_PRESERVED` never is;
duplicates fail `EXACTLY_ONCE`; a lost delivery fails the attempt tests (`ATTEMPTS`, `FIRST_ATTEMPT`) but not `EXACTLY_ONCE`;
`SURVIVORS` never fail. A new contract test that asserts an order, a count or an attempt is not in a set: it goes unnoticed by
these checks, and `python -m tests.test_bus_faults_contract` lists it.

What this evidence does *not* cover: a **late redelivery**. A nack keeps the message's dispatch key, so `lose_delivery` and
`ack_lost` always redeliver next in the lane, before any other lane's later message; a real redelivery after an ack timeout comes
after other lanes moved on. So "results identical under `lose_delivery`" (the backtest runs below) says nothing about redelivery
interleaved with other lanes; `delay` and `duplicate_publish` cover late arrival of first deliveries and of copies, not of
redeliveries. Group-member failover is not modelled either (grouped subscriptions are never crashed; a muted member is cancelled
and its lanes pass to the others at once).

## Where the current phase protocol breaks under each fault

From reading `pluto/control/phases.py`, `controller/participants.py`, `controller/controller.py`, `controller/process_manager.py`
and `processes/endpoint.py` (master `dc3e9aa4`), then checked by running whole `buy_and_hold` backtests (the 2008 example, a
strategy, serialising wrapper inside the fault bus) with one fault at a time (probability 0.1, 3 seeds; scratch script, not committed;
`delay` 0.3). Results: `ack_lost`, `duplicate_publish`, `crash`, `mute`, `publish_ack_lost`, `publish_fail`: every run aborted with a
`PhaseError` (or a raw `PublishFailed`) within the first ticks; `delay` (about 22,800 delayed messages) and `lose_delivery` (about
8,300 lost deliveries): results identical to the run without faults. (`mute` aborted with `'no done from broker'` or an order-count
mismatch: a muted subscription of the broker to `phase.done.trade` never learns the counts, which is the "second subscription"
finding below.)

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

## Review round (agent `review-faultbus`, 7 findings, all fixed)

| # | Fix |
|---|---|
| 1 High | contract-under-faults sets were seed luck: probability-1 profiles, `ORDER` (now 9, with `test_replay_then_live_messages_follow_the_dispatch_rule`) in the union of a delay, `FIFO_PRESERVED` never fails under a delay, `SURVIVORS` 25 (the two `attempt == 1` tests moved to `FIRST_ATTEMPT`), all counts corrected here and in the module docstring |
| 2 | a fixed number of draws per call (7 per publish, 5 per delivery); test of equal RNG state across configurations |
| 3 | a crash subscribes the replacement first, cancels after; `QueueFull` skips the crash (`crash_skipped`); tested with a small `queue_capacity` |
| 4 | `mute` cancels the subscription with the delivery unsettled (a hung consumer never acks); `unmute` restarts at the checkpoint (exact, ungrouped topics) and a muted pattern or group member is permanent (`unmute` raises); tested: the unmuted participant gets what it missed |
| 5 | tests: crash on the 4th of 5 messages replays from the 4th (a mutant resuming at `EARLIEST` fails it), the redelivery budget survives a crash (a mutant keyed by subscription id fails it); new `SeededFaultBus.crash()`. To make a checkpoint always exist, a subscription that started at the end of the log resumes at the first message logged after it |
| 6 | "late redelivery" and "group-member failover" in the not-modelled list, with what it means for the `lose_delivery` evidence |
| 7 | unused import removed; `start=` from the receipt of a dropped message raises `ValueError`; `publish_fail` validates first; tests for `release_failed` and the `QueueFull` wait; `subscribe` and `Request` flushing documented; exports and the stale docstring line in `pluto/bus/__init__.py` |

## Integration on top of `stage2-bus` (`cfca03fd`)

The branch was rebased onto the local `stage2-bus` (`git rebase --onto stage2-bus dc3e9aa4 stage2-faultbus`; one conflict, the
docstring and imports of `pluto/bus/__init__.py`). The earlier sections describe the wrapper before the rebase; this is what the new
interface changed.

**`SeededFaultBus` on the new `Bus` interface** (`pluto/bus/faults.py`):

| Interface item | What the wrapper does |
|---|---|
| `networked` | `inner.networked`, or True with `FaultConfig(release_on_poll=True)`. Reason: `PhaseDriver.drive` loops `run_until_idle`, "everything in?", `if not bus.networked: stop`, else `poll`. Its `run_until_idle`/`step` skip ahead in logical time until nothing is held, so by default nothing can arrive after idle and a missing done is the immediate error it should be: `networked` is then the inner bus's (False over `InMemoryBus`). In the new mode `release_on_poll` the wrapper returns idle while messages are held and releases them only in `poll`, so messages can arrive after idle: `networked` is True and the driver waits with its deadline. That is the mode in which a delay can outlive `run_until_idle` over an in-memory inner bus; it is what tests a driver that waits |
| `poll(timeout)` | not networked: False at once. Otherwise release what is due (`release_on_poll`: first skip ahead in logical time to the next held message), then `inner.poll`; True if either moved something; never runs a handler. With `release_on_poll` over an inner bus that cannot wait, a poll that found nothing sleeps `timeout` (the driver's deadline is wall-clock: otherwise a busy loop). `BusClosed` after close |
| `Receipt` class | a held message's receipt is `Receipt(msg_id, resolver)`: asking for `position` is a barrier (puts everything held into the log, like `subscribe`) and returns the inner position (`BusError` if the message was dropped on release; before: `ValueError` at `subscribe`). A direct publish returns the inner receipt; the first-logged checkpoint of a subscription that started at the end of the log keeps the inner **receipt** unresolved until a restart needs its position (a networked bus does not wait in `publish`; test with receipts that count their resolutions) |
| S15 topics | `_validate` goes through `exact_topic`, which refuses whitespace and control characters: a held message is refused at `publish`, not on release (test) |
| `close()` | holds cleared, then `inner.close()`, whose error goes through (test with an inner whose `close` raises) |

Tests: `tests/test_bus_faults.py` has the transparent contract over **three** implementations now (memory, serializing, NATS: `JetStreamTransparentContract`, skipped without a server, a failure with `PLUTO_REQUIRE_NATS=1`) and tests for each item above, `release_on_poll` (a delayed message arrives only on poll, a poll runs no handler, without the mode a poll releases nothing). `tests/test_bus_faults_contract.py` classifies the five contract tests added on `stage2-bus` (88 contract tests, hash-seed one left out): `test_poll_reads_the_log_and_never_runs_a_handler` counts invocations (fails under every fault but a delay: in `ATTEMPTS`); the other three that run (`test_a_bus_nobody_else_writes_to_has_nothing_to_poll`, `test_poll_after_close_raises`, `test_topics_with_whitespace_or_control_characters_are_refused_by_every_bus`) pass under all faults (`SURVIVORS`, now 28); `test_poll_finds_a_message_published_by_another_bus_of_the_same_log` skips on these buses. Measured failing sets now: `delay` 9, `duplicate_publish` 55, `ack_lost` 53, `lose_delivery` 22, `crash` 45; 28 pass under all (29 with the skipped one).

### Whole backtests with the new driver (`buy_and_hold`, 2008 to 2013, 3 seeds each, `tests/test_bus_faults_runs.py` is the short committed version)

Setup: `SeededFaultBus(SerializingBus(InMemoryBus))` as the bus of the run (`process_manager.InMemoryBus` patched, as `tests/test_pluto_phases_run.py` does), `PhaseDriver` deadline patched to 2 s (1 s in the test), a 150 s alarm per run, one fault at a time (probability 0.1; `delay` 0.3 with `max_delay` 6; `mute` 0.02), in both modes: **default** (`networked` False: the driver never waits) and **poll** (`release_on_poll`: `networked` True, the driver waits with `poll`; the in-memory inner bus needs this mode for the waits to matter, because by default the wrapper never returns idle while holding a message). Outcome per run, compared with the run without faults at exact tolerance:

| Fault | default mode | poll mode |
|---|---|---|
| `delay` (22,700 delayed messages) | **identical** (3/3) | **identical** (3/3); also `delay` 1.0 with `max_delay` 30 (all 75,888 messages delayed): identical 3/3 |
| `lose_delivery` (9,000 lost deliveries) | **identical** (3/3) | **identical** (3/3); also delay 0.5 + `lose_delivery` 0.3 together (37,800 delayed, 27,200 lost): identical 3/3 |
| `ack_lost` | `PhaseError ... said done twice` (3/3; the broker and/or a runtime; phase account, submit or book) | `PhaseError`: `said done twice` (2/3), `broker sent a done of phase.submit at <earlier tick>` (1/3: a stale done) |
| `duplicate_publish` | `PhaseError ... broker said done twice` / `<runtime> said done twice` (3/3) | `PhaseError ... sent a done of phase.<x> at <tick>` (3/3: late copies of dones arriving in a later phase; the broker's or a runtime's) |
| `crash` | `PhaseError ... said done twice` (3/3) | the same (3/3) |
| `mute` | `PhaseError ... no done from <runtime or broker>` at once (3/3) | `PhaseError ... no done from <...> within 2 s` (3/3): the driver's deadline, named participants |
| `publish_ack_lost` | `PhaseError ... said done twice` (2/3), raw **`PublishFailed`** from `phase.submit` (1/3) | the same |
| `publish_fail` | raw **`PublishFailed`** from `phase.account` (1/3), `PhaseError ... failed: PublishFailed ... strategy.orders` (1/3), `PhaseError ... sent 0 orders in 1 batches, the broker received 0 in 2` (1/3) | the same |

**No wrong result and no hang in any run** (48 runs plus 6 heavy-delay ones; every outcome is an identical frame or an abort). Aborts are
expected until the handlers are idempotent. Worth reporting about them:

* **The new driver tolerates a delay and a lost delivery** in both modes, which the old one did only by accident of `run_until_idle`
  (the earlier experiment): in poll mode `run_until_idle` returns with messages held and the driver's wait, `announced_orders` and the
  counts in the submit event carry the run to the identical frame, with every message delayed up to 30 ticks. That is the evidence
  that the explicit waits of `stage2-bus` work. It does **not** cover a late *redelivery* across lanes (not modelled, see above).
* **A raw `PublishFailed` escapes the controller** (`publish_fail` 1/3, `publish_ack_lost` 1/3): the driver's own publish of a phase
  event is not retried and not wrapped in a `PhaseError`. An abort, but not a clean one. Idempotent handlers need a publish retry with
  the same `msg_id` (the "retry" items of the analysis above).
* **A late duplicate done is an abort** in poll mode (`sent a done of phase.<x> at <earlier>`): the driver treats a done that is not of
  the phase and tick as an error, not as an already-counted copy. With deterministic `msg_id`s on dones it can ignore a copy.
* `mute` of the broker's subscription or a runtime's gives `no done from ...` in default mode, and in poll mode the same after the
  deadline with the names; a muted participant is the case the deadline exists for.

`tests/test_bus_faults_runs.py` (7 s, 16 subtests, a short `buy_and_hold` to 2008-02-15): for each fault and mode, the outcome must be
the baseline frame or a `PhaseError`/`PublishFailed`, `delay` and `lose_delivery` must be identical, a fault must have been injected,
`networked` must be what the mode says, and an alarm cuts a hang. When handlers become idempotent more faults move to identical and the
test still passes; add them to `TOLERATED` then.

## Final run

Fresh clone of the rebased `stage2-faultbus` at `0a251a9b` (`--no-hardlinks`), new env from `dev/setup_env.sh` (with `nats-py`), NATS up at
127.0.0.1:4222, `PLUTO_REQUIRE_NATS=1`, whole `tests/`: **959 passed, 5 skipped** (291 s), and under `PYTHONHASHSEED=1`: **959 passed, 5 skipped**
(286 s). The skips are the legitimate ones: `test_poll_finds_a_message_published_by_another_bus_of_the_same_log` on the two in-memory
buses and on the two wrappers over them (4), and the release-failure test on the in-memory implementation (needs a codec to refuse a
payload). `stage2-bus` alone: 590 passed, 2 skipped. The clone and its env were removed.
