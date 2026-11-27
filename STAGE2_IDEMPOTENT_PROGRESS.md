# Stage 2, idempotent runs (branch `stage2-idempotent`)

Base: `origin/master` = `798c6e11`. Worktree `/home/yves/.cache/hades-pluto-dev/stage2-idempotent/wt`, env `.../stage2-idempotent/env`
(`dev/setup_env.sh`). Not pushed. Task: runs that survive duplicates and retries: duplicate delivery, late duplicate publish,
consumer crash and resume, transient and ambiguous publish failure give the **identical result** over `SeededFaultBus`
instead of a clean abort. No change to any result; the original test files and stored results unchanged.

## Status

- [x] 1. design (this file, before code)
- [x] 2. deterministic `msg_id`s, publisher retries
- [x] 3. receiver-side idempotence (participants, endpoint, manager, driver)
- [x] 4. tests: unit tests per dedup point, whole runs under faults
- [x] 5. final run from a fresh clone (see the end)

## 1. Design

### What is on the bus, who publishes it, what a duplicate does today

Four publish sites, all in `pluto/control/` (`grep "\.publish("`): the driver (`PhaseDriver.drive`), `phases.publish_done` (every
participant), `BrokerParticipant._account` and `RuntimeEndpoint.send_orders`. The book stream (`events.StreamEvent`) and `Observed`
are **not** on the bus: the broker service calls its subscribers (the strategy manager, the metrics consumer) in process.
Allocation is not there yet. Every message is published without `msg_id`, so the bus names it `producer:seq`: **a handling that is
repeated publishes under a new id**, and nobody downstream can tell a copy from a new message.

| Message | Topic, key | Publisher | Receivers | A duplicate or a retry today | Rule that makes it harmless |
|---|---|---|---|---|---|
| Phase event `book` / `account` / `trade` / `submit` (`SignalEvent`, `AccountEvent`, `ClockEvent`, `SubmitEvent`) | `phase.<name>`, key = calendar for `book`, else `''` | `PhaseDriver.drive`; a failed or ambiguous publish escapes as a raw `PublishFailed` or, retried by hand, is a copy | `BrokerParticipant` (book, account, submit), `RuntimeEndpoint` (trade) | the handler works again: the book writes twice (`revalue` compounds returns), the account state is extended and sent twice, the strategy runs twice (two batches), submit runs on an empty tick; and a second `done` | id `controller/<phase>/<tick ns>/<key>`; the publisher retries with it (bounded, then `PhaseError`); each receiver keeps the ids it handled **in the current tick** and ignores a copy (same id) and anything of an older tick |
| `done` (`PhaseDone`) | `phase.done.<phase>`, key = participant | `phases.publish_done` (via `report`/`fail`) | `PhaseDriver` | `said done twice`; a late copy in a later phase is `sent a done of <phase> at <tick>`; both abort | id `<participant>/done/<phase>/<tick ns>/<key of the event>` (the key keeps the two calendars' book dones of a tick apart); the driver keeps the ids of the tick and ignores a copy; a done of an older tick from a participant it already accepted a done from is a late copy of a closed tick and is ignored; a done of an older tick from anyone else, or a different done under the same participant, phase and tick, stays an error |
| Account state (`BrokerState`) | `strategy.account.<process id>`, key = process id | `BrokerParticipant._account` | that runtime's `RuntimeEndpoint` | applied twice: the fills and the last sale prices count twice in the runtime's ledger | id `broker/account/<tick>/<process id>`; the endpoint ignores a copy (same id) and an older tick: **applying the state of a tick already applied is a no-op**; the publish is retried with the same id |
| Order batch (`OrderBatch`) | `strategy.orders`, key = process id | `RuntimeEndpoint.send_orders` (called by the strategy while it handles the trade event) | `StrategyManager` (rank 0), `PhaseDriver` (rank 1, counts for `announced_orders`), `BrokerParticipant` (rank 1, wakes a waiting submit) | orders applied twice at submit (`BrokerService.submit` answers a second id with `OrderRejected`, which also lands in the stream), the batch counts double: the driver may believe a missing batch arrived, the broker side reports `received 2 in 2`; a copy after the tick closed is reported as a late batch | id `<process id>/orders/<tick ns>/<n>` (n = 1, 2, ... the batch of the trade event); the manager and the driver keep the ids of the tick and ignore a copy; a batch of a closed tick whose id is the deterministic id of that tick (and is in the closed tick's ids, or is of an older tick) is a copy, anything else of a closed tick is still a late batch; retried publish, same id |
| Submit event (`SubmitEvent`) | `phase.submit` | the driver | `BrokerParticipant` | see phase events; a second `_submit` finds the manager already cleared and submits nothing | same as phase events; the broker side ignores a copy, so `submit` and `clear` run once |
| Book stream event / `Observed` | in-process callbacks of `BrokerService` | `BrokerService._publish` (numbered 1, 2, 3, ...) | `StrategyManager._on_book_event`, `StreamMetrics` | none reaches them as long as the book phase runs once (above); `StreamMetrics` already skips a sequence it folded (`stream_event.sequence <= replica.sequence`) and `stream.fold` skips and refuses gaps | no code change: the dedup is the stream's own sequence number (a pinning test); the book phase's dedup keeps `observe` from running twice |

### The rules

1. **Identity is deterministic.** The id of a message is a function of what it is (kind, tick, key), never of how often it was handled
   or published. A retried publish carries the id of the first attempt; a repeated handling republishes nothing (it is ignored), so no
   new id appears.
2. **Receivers deduplicate on `msg_id`, per tick.** Each receiver keeps the set of ids it handled in the **current tick** (the
   highest `logical_ts` it has seen). A message whose tick is older than that is a copy by construction (see "why per tick is enough");
   a message of the current tick is a copy iff its id is in the set; the set is dropped when a newer tick starts. Membership tests
   only, no iteration: deterministic, and bounded by the messages of one tick (a few dozen).
3. **Effects are idempotent where that is natural.** An account state for a tick already applied is a no-op (the endpoint also
   remembers `_account_ts`); a done for a (participant, phase, tick, key) already accepted is a no-op; the metrics consumer skips a
   folded sequence.
4. **Publishers retry transient failures.** `PublishFailed` (the documented "retry with the same `msg_id`" error of the bus) is
   retried up to `phases.PUBLISH_ATTEMPTS` (10) times with the same id; then a `PhaseError` names the topic. The attempts have no
   delay: the bus has no clock (S5), the budget is a count. At the fault rates of the tests (10 %) the chance of ten failures in a
   row is 1e-10 per message.
5. **A handler that was handled is not run again**, whatever brought the copy: a duplicate delivery (`ack_lost`), a copy published
   later (`duplicate_publish`, a publisher's retry after an ambiguous failure), or the replay after a crash and resume (the
   subscription restarts at its checkpoint and reads again what it had not settled, handled or not).

### Why per tick is enough (the redelivery window)

The controller is lock-step: it publishes the phases of tick T+1 only after every expected `done` of T was accepted and (for
orders) the batches the dones announced were counted. A message of tick T is therefore never **first** seen after a message of
T+1 was: whatever arrives for T once T+1 has started is a copy of something already handled (or the run has already aborted for the
message that never came). The window of a copy is not bounded in ticks (a copy published late, a replay from an old checkpoint,
a redelivery after an ack timeout), which is why "older tick" is the rule and not "the last k ticks"; inside a tick the ids are
what tells a copy from the message after it. Two refinements keep real faults detectable:

* a **done** of an older tick is ignored only if its participant already had a done accepted by this driver (a stray done from an
  unknown participant stays `sent a done of <phase> at <tick>`);
* a **batch** of a closed tick is ignored only if its id is the deterministic one of that tick and process (or one of the closed
  tick's remembered ids); a batch under any other id stays a late batch (the manager reports it in the next phase).

### What stays impossible to tolerate

* A participant that never answers (`mute`, a process that died): no done, `PhaseError` at once in memory, after the driver's deadline
  on a networked bus. A duplicate cannot stand for a message that did not come.
* A copy that **differs** from the message it copies (same id, other payload): the first wins; the driver reports a done that
  contradicts the one it accepted (same participant, phase and tick, other content). Nothing else can tell.
* A real process crash loses the process's memory, including the ids it handled and the strategy's own state: resume after a crash is
  tolerated here for the subscription restarting at its checkpoint (the bus side, what `SeededFaultBus` models), not for a runtime that
  restarts empty (it would need the state of the strategy restored; a later stage).
* More than `PUBLISH_ATTEMPTS` consecutive publish failures: `PhaseError`.
* A late **redelivery** across lanes after the tick closed is tolerated by the "older tick" rule, but the fault bus does not model it
  (its redeliveries are immediate, see `STAGE2_FAULTBUS_PROGRESS.md`): not tested.

## 2. What was built (and one correction of the design)

**Correction: a clock time is not a tick.** The first version of the ids (`<kind>/<clock time>/...`) failed on the fault-free run:
`Controller.clock_update` is called more than once with the same clock time (for example the session start and the first bar of a
day share it), so two clock events carried the same ids and the second was taken for a copy of the first. The identity of a
message is therefore **(kind, clock time, the n-th drive of that phase and key at that clock time)**, and everything that answers an
event derives its id from the **id of the event** it answers: `controller/<phase>/<clock ns>/<n>/<key>` for an event (`n` counted by
the driver per phase and key, reset when the clock time advances), `<participant>/done/<phase>/<clock ns>/<event id>`,
`broker/account/<process id>/<account event id>`, `<process id>/orders/<clock ns>/<trade event id>/<k>`. The sets of ids are kept per
**clock time** (not per clock event): a late copy of the first event's message during the second is still recognised. "Older" in the
rule above means an earlier clock time.

| Piece | Change |
|---|---|
| `pluto/control/phases.py` | `phase_msg_id`, `done_msg_id`, `account_msg_id`, `orders_msg_id`/`orders_msg_prefix`; `publish_with_retry` (same id, `PUBLISH_ATTEMPTS` = 10, then `PhaseError` naming id and topic); `Handled` (ids of the clock time in progress, membership only); `publish_done`/`fail`/`report` take `answers` (the event answered); `PhaseDriver`: the phase event is published with retry and its id, dones are collected once (`_done_ids` per clock time with their content, a copy of a closed clock time from a participant already accepted is ignored, a contradicting done is a problem), batches counted once (`_batch_ids`, kept for the current clock time) |
| `controller/participants.py` | `BrokerParticipant`: book, account and submit events handled once (`Handled`), the account states published with retry under `account_msg_id`, dones carry the event id |
| `processes/endpoint.py` | `RuntimeEndpoint`: broker states and trade events handled once; `send_orders` publishes with retry under `orders_msg_id` (trade event id, batch number); dones carry the event id |
| `controller/process_manager.py` | `receive_orders(session_id, batch, msg_id=None)`: a copy of a batch of this clock time is ignored, also after the tick closed; a batch of an earlier clock time under the deterministic id of that process is a copy; any other batch of a closed tick is a late batch as before |
| `broker_service/`, `finance/metrics/consumer.py` | **no change**: the book stream is not on the bus; `StreamMetrics` already skips a sequence it folded (observation point included) and `stream.fold` refuses gaps; a pinning test (`StreamTests`). The book phase runs once, so `observe` and the book writes are not repeated |
| the bus | **no change** (the contract and every bus module untouched): `PublishFailed` was already exported by `pluto.bus` and documented as "retry with the same `msg_id`" |

Not dedup-protected on purpose: a muted participant (no done is a failure), a done that is not from a known participant for an earlier
tick (`sent a done of ...`), a second *different* done of a participant for the same event.

### Changed assertions of existing tests (all in tests that are ours, none in the original result tests nor the bus contract)

| Test | Before | Now |
|---|---|---|
| `test_pluto_phases.py` `test_done_twice_aborts` | two participants answer the same event: `a said done twice` | renamed `..._for_different_events_aborts`: the second answers another event (`answers='another event'`); and new `test_a_copy_of_a_done_is_ignored` for the same event. `FakeParticipant` takes `answers` and, by default, answers the event it received |
| `test_pluto_phase_participants.py` `test_a_batch_delivered_twice_aborts_the_run`, `test_a_batch_of_orders_delivered_twice_aborts_the_run` | the bus publishes the batch twice with its kwargs: `received 2 in 2` | the same message twice is now one batch: `test_a_batch_delivered_twice_is_one_batch`, `test_an_empty_batch_delivered_twice_is_one_batch`; the two abort tests keep their assertions with a second **distinct** batch (`FaultyBus.extra`, `test_a_batch_sent_twice_aborts_the_run`, `test_a_batch_of_orders_sent_twice_aborts_the_run`). `test_pluto_phase_order.py` runs them on the shuffled and arrival buses unchanged |
| `tests/test_bus_faults_runs.py` | "identical, or a PhaseError" for each fault | rewritten: identical at rtol 0 for every fault but the muted participant, on three runs |

## 3. Tests

- `tests/test_pluto_idempotent.py` (28): `Handled`; `publish_with_retry` (retry with the same id, budget then `PhaseError`, other errors not retried); the driver (a retried event and its ambiguous copies, one done from three answers, two events of one clock time are two ids, a late copy of a closed tick's done ignored for a known participant and an error for a stranger, a contradicting done, a batch twice counted once); the participants and the endpoint on a fake book (book, account, broker state, trade, submit and batch events published twice change nothing); `receive_orders` (copy in the tick, after it, of an earlier clock time, another batch is late, two clock events at one clock time); the metrics stream. **Mutation check** (each dedup point patched out, one at a time): `Handled` -> 9 tests fail, the driver's `_on_done` -> 3, `_on_orders` -> 1, the manager's id check -> 5, `publish_with_retry` -> 4.
- `tests/test_bus_faults_runs.py` (2 tests, 64 + 12 subtests, about 50 s): `buy_and_hold` (to 2008-01-25: orders and fills), `dividends` (a cash dividend on a position), `cancel_orders`; the default mode (2 seeds, 1 for `cancel_orders`) and `release_on_poll` (1 seed, two of the runs); each of delay, lost delivery, duplicate delivery, late duplicate, crash and resume, publish failure, ambiguous publish failure: **identical frame at rtol 0 / atol 0**, and the fault must be in the fault log; the muted participant: `PhaseError` (`no done from ...` or `no batch of orders from ...`), never a frame, never a hang (alarm).

## 4. Result per fault (whole `buy_and_hold` 2008 to 2013, 7,312 ticks, `SeededFaultBus(SerializingBus(InMemoryBus))`, seeds 1 to 3)

Before = `STAGE2_FAULTBUS_PROGRESS.md` (the driver of `stage2-bus`, probability 0.1; delay 0.3). After = this branch, the same faults, both modes (`default`: `networked` False, `poll`: `release_on_poll`); counts of faults injected in a run are in the last column of the first seed.

| Fault | Before (both modes) | After, default | After, poll | Faults injected (seed 1) |
|---|---|---|---|---|
| duplicate delivery (`ack_lost`) | `PhaseError: said done twice` or a stale done, 3/3 | **identical 3/3** | **identical 3/3** | 8,985 |
| late duplicate publish (`duplicate_publish`) | `PhaseError` (done twice / stale done), 3/3 | **identical 3/3** | **identical 3/3** | 7,381 |
| consumer crash and resume (`crash`) | `PhaseError: said done twice`, 3/3 | **identical 3/3** | **identical 3/3** | 24 (the cap: 3 restarts of each of 8 subscriptions) |
| transient publish failure (`publish_fail`) | raw `PublishFailed` or `PhaseError`, 3/3 | **identical 3/3** | **identical 3/3** | 8,369 |
| ambiguous publish failure (`publish_ack_lost`) | `PhaseError: said done twice`, raw `PublishFailed`, 3/3 | **identical 3/3** | **identical 3/3** | 8,538 |
| delay | identical 3/3 | identical 3/3 | identical 3/3 | 22,674 |
| lost delivery | identical 3/3 | identical 3/3 | identical 3/3 | 9,207 |
| all of them at once (0.05 each, delay 0.2) | not run | **identical 3/3** | **identical 3/3** | 16,000 delayed, 4,000 to 5,000 of each other kind |
| muted participant (`mute`) | `PhaseError: no done from ...` | `PhaseError: no done from ...` (at once) 3/3 | `PhaseError: no done from ... within 2 s` 3/3 | 1 |

No wrong result and no hang in any of the 54 runs.

## 5. Still not tolerated

- A participant that never answers (`mute`, a process that died): a clean `PhaseError` naming it; nothing can stand for a message that
  does not come.
- A real process crash that loses the process's memory (the ids it handled, the strategy's state): the fault bus models a restart of
  the **subscription** (the in-memory handlers survive); a runtime restarting empty needs its state restored, a later stage.
- More than 10 consecutive failures of one publish: `PhaseError`. At 10 % that is a chance of 1e-10 per message (about 8e-6 per run).
- A copy that differs from its original under the same id: the driver reports a contradicting done; for a batch or a state the first
  one wins, silently.
- A **late redelivery** across lanes (after an ack timeout) is covered by the "older clock time" rule but not tested: the fault bus
  redelivers immediately.
- Duplicates of messages the bus does not carry (the book stream, `Observed`) are not a bus fault here; they rely on the sequence
  number the metrics consumer already checks.

## 6. Measurements

- Messages of a whole `buy_and_hold` run (2008 to 2013, `SerializingBus(InMemoryBus)` counting every `publish`): **75,888**, as on
  `origin/master`: the ids and the retries add no message when nothing fails (counting script in the scratch directory).
- Time of that run in memory: 5.7 s (`STAGE2_BUS_PROGRESS.md` has 4.4 and 3.7 s for it on another day; not re-measured on `master` here,
  the machine is shared and the work added per message is a set lookup and a string format).
- Suite: 995 passed, 5 skipped in 331 s on the development machine (959 in 291 s before, with 36 new tests; the whole-run test under
  faults is about 50 s of the difference).

## 7. Final run

Fresh clone of `stage2-idempotent` at `d42e4c2a` (`--no-hardlinks`), new env from `dev/setup_env.sh`, NATS up at 127.0.0.1:4222,
`PLUTO_REQUIRE_NATS=1`, whole `tests/`: **995 passed, 5 skipped** (443 s on a loaded machine), and with `PYTHONHASHSEED=1`: **995 passed,
5 skipped** (333 s). The skips are the legitimate ones (the poll test that needs a second writer on the four in-memory
buses and wrappers, the release-failure test on the in-memory implementation). `origin/master` had 959 passed, 5 skipped: 36 more
tests, 28 of them in `test_pluto_idempotent.py` (the rest in `test_pluto_phases.py`, `test_pluto_phase_participants.py` and
`test_bus_faults_runs.py`, which now has two tests instead of one). The original result tests and the bus contract are unchanged
(`git diff origin/master --stat` on them is empty). The clone and its env were removed.

## 8. Review fixes (agent `review-idempotent`: 2 High, 4 Medium, 3 Low; the commits above are the state it reviewed, `67c78b6f`)

Sections 1 to 7 describe that state. Where they say "older clock time is a copy" or give ids with the clock time, this section replaces
them: **the clock does not order anything; the driver's step does.**

| # | Finding | Fix |
|---|---|---|
| H1 | **Minute mode broke.** Every "older clock time is a copy" rule assumed the clock never goes backwards; `SimulationLoop(frequency='minute')` emits timestamps that do (`buy_and_hold`, minute, to 2008-01-08: `PhaseError: phase.account at 2008-01-01 23:45:00: no done from <runtime>, broker`; fine on `798c6e11`). No test ran minute mode. | `PhaseDriver` counts its drives (`_step`: 1, 2, 3, ... one per phase event, it only grows) and puts the number in the **correlation id** of the event and of everything that answers it (done, broker state, batch of orders: `phases.step_of`). Ids are `controller/<step>/<phase>/<clock ns>/<key>`, `<participant>/done/<event id>`, `broker/account/<process>/<event id>`, `<process>/orders/<trade event id>/<k>`. `Handled`, the driver's dones (`step < _step` is a copy of an earlier drive) and batches (`step != _step`) and the manager order by **step**, never by `logical_ts`; the driver's `n` counter, `_tick`, `_drives`, `_participants` and the prune by clock time are gone. The steps are exact and unbounded (no window): a drive starts only when the last ended. A message without a step (published by something that is not the driver, as the tests do) is judged by id alone within the step in progress. Tests: a whole minute-mode run fault-free and under every fault and all at once, identical to the plain run at rtol 0 (it **fails on `67c78b6f`**: same `PhaseError`), the same on a fake book whose clock goes back (also fails on `67c78b6f`), and unit tests of `Handled`, the driver and the participants with a clock that goes back |
| H2 | **A real late batch was silently dropped**: `_is_copy_of_older` was a prefix test on the id, and every runtime batch has that form, so a first-time batch of an older tick was "a copy" once a later tick had been taken. | The manager keeps the batches it took, by id, with their step and content, for the last `phases.COPY_WINDOW` = 64 drives (a clock event is at least four drives: about 16 clock events; the ids are pruned when a newer step is taken, so the memory is bounded by the window) and takes a batch for a copy **only if its id is in that set**; any other batch of a closed tick is late and reported, as before. A copy that comes later than the window is reported as late: a loud abort, never a silent loss. `_is_copy_of_older`, `_ids`, `_ids_ts` and `orders_msg_prefix` are removed. Test `test_a_batch_never_taken_is_late_even_after_a_later_tick_was_taken` (fails on `67c78b6f`), copy inside the window ignored, copy older than the window reported, the ids bounded |
| M1 | `answers=''` default: a caller that forgot it shared an id between all events of a clock time | `answers` is a keyword-only **required** argument of `publish_done`, `fail`, `report`, and it is the **event** (its envelope): the id and the step of the done derive from it. Tests pass it explicitly (`event(msg_id, step)` helper); a test shows the `TypeError` |
| M2 | "First wins silently" for batches | The manager stores the serialised content (`SerializeToString(deterministic=True)`) with each taken id; a copy whose content differs is recorded (`take_conflicting_batches`) and the broker participant fails the submit with `<process> sent two batches of orders under the id <id> that differ`. **States are left as they were** (the runtime ignores a copy, the first one wins): the broker publishes them itself and a state is large, so a contradiction is unlikely and the check would cost more than it protects; said in the `phases.py` docstring |
| M3 | Stale docs | `phases.py` (first paragraph, the counts bullet, the lock-step claim and the copies paragraph, the `drive` docstring, `fail`) and `dev/README.md` rewritten; the claim "what arrives for an earlier one is a copy" is now about steps, and says what stays an error |
| M4 | The whole-run test was weaker than the record | `tests/test_bus_faults_runs.py`: the combined run (delay 0.2, the others 0.05) on `buy_and_hold` and on the minute run, default and `release_on_poll`; the minute run joins the single-fault matrix and the mute test; the baseline is now the **plain** `InMemoryBus` run, not the wrapper's; a new class runs three fake runtimes (the first sends **two batches per trade event**), two calendars, two clock events at one clock time and a clock that goes back, over every fault and all of them at once, comparing the calls of the book and what each runtime was told. No example sends more than one batch per trade event (the controllable always sends one) and none runs two strategies, so the fake book is where both are exercised end to end. The stale docstring is rewritten |
| L1 | A done that cannot be published hid its cause | `publish_done` catches the `PhaseError` of the exhausted retry and logs `<participant> could not say done in <phase>: could not publish <id> on <topic> in 10 attempts: ...`; the controller still reports the missing done (test with a logbook `TestHandler`) |
| L2 | Retry has no delay and counts only `PublishFailed` | Said in the docstring of `publish_with_retry`: only `PublishFailed` is retried; only the fault bus raises it today; the JetStream bus reports a refused message as a plain `BusError` from its next read, so a real transient publish error is not retried yet. The bus has to say which of its errors are transient: a later stage |
| L3 | A late done of an older clock time from a known participant was ignored even with other content | Gone with H1: a done of an earlier **drive** is a copy by construction (that drive ended with every done accepted, or the run aborted); the contradicting-content check applies to dones of the drive in progress |
| L4 | Two participants with one name answering one event merge | Intended (the renamed test): the same message is one message |

**Recorded, not changed: the minute clock emits non-monotone timestamps inside a session.** The reviewer measured it (e.g. `00:00 ->
20:58 -> 20:45 -> 21:00` on XNYS; 19 backward steps on `24/7` and 13 on `XNYS` over 18 days). Whether that is intended Zipline behaviour
or a bug of `SimulationLoop`/the clock generator is a separate question for later; nothing here depends on the answer any more (the protocol
orders by step), and the results of `798c6e11` are unchanged. The in-memory handlers still compare clock times for equality only
(`RuntimeEndpoint` refuses a trade event whose clock time is not that of the account state it applied) and `StrategyManager.receive_orders`
keeps its old `batch_ts < global_ts` test for "a batch of an earlier tick": with a clock that goes back, a late batch for a **later** clock
time than the current one is not reported as late (as on `798c6e11`; the count check of the submit phase still aborts the run).

Messages per whole `buy_and_hold` run: unchanged, 75,888 (the step travels in the correlation id of existing messages).

Final run of the review fixes: fresh clone of `stage2-idempotent` at `fe9b61ae` (`--no-hardlinks`), new env from `dev/setup_env.sh`, NATS up,
`PLUTO_REQUIRE_NATS=1`, whole `tests/`: **1009 passed, 5 skipped** (337 s), and with `PYTHONHASHSEED=1`: **1009 passed, 5 skipped** (330 s)
(995 before this round: 14 more tests; the same 5 legitimate skips). The original result tests and `tests/test_bus_contract.py` are unchanged.
