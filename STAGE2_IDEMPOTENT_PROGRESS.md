# Stage 2, idempotent runs (branch `stage2-idempotent`)

Base: `origin/master` = `798c6e11`. Worktree `/home/yves/.cache/hades-pluto-dev/stage2-idempotent/wt`, env `.../stage2-idempotent/env`
(`dev/setup_env.sh`). Not pushed. Task: runs that survive duplicates and retries: duplicate delivery, late duplicate publish,
consumer crash and resume, transient and ambiguous publish failure give the **identical result** over `SeededFaultBus`
instead of a clean abort. No change to any result; the original test files and stored results unchanged.

## Status

- [ ] 1. design (this file, before code)
- [ ] 2. deterministic `msg_id`s, publisher retries
- [ ] 3. receiver-side idempotence (participants, endpoint, manager, driver)
- [ ] 4. tests: unit tests per dedup point, whole runs under faults
- [ ] 5. measurements, final run

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
