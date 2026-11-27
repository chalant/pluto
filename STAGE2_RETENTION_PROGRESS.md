# Stage 2, bus log retention (branch `stage2-retention`)

Base: `origin/master` = `798c6e11`. Worktree `/home/yves/.cache/hades-pluto-dev/stage2-retention/wt`, env
`/home/yves/.cache/hades-pluto-dev/stage2-retention/env`. Not pushed. Scope: `pluto/bus/`, its tests, `dev/` benches and docs; nothing in
`pluto/control/`, `pluto/broker_service/` or `pluto/finance/metrics/consumer.py` (another worker, idempotent handlers).

## Status

- [x] 1. measurement (before)
- [ ] 2. design (below)
- [ ] 3. implementation, contract tests
- [ ] 4. measurement (after), final runs

## 1. Measurement before (`dev/bench_bus_memory.py`, `dev/bench_bus_memory_run.py`)

The deep size of every attribute of the bus (containers, envelopes, strings, bytes; functions and classes not followed; a
subscription's back-pointer to its bus not followed; the connection of a `LogBus` left out), shared objects counted once. Payloads are
bytes (a `SerializingBus`). Machine shared; sizes do not depend on load, times do.

| Run | Messages | `_logs` | `_tokens` | everything else | Sum | Per message |
|---|---|---|---|---|---|---|
| `buy_and_hold`, in memory (`SerializingBus(InMemoryBus)`) | 75,888 | 30.7 MB | n/a | 26 KB (consumers/lanes/cursors 24 KB, routes, subscriptions) | 30.8 MB | 405 B |
| `buy_and_hold`, over NATS (`LogBus`) | 75,888 | 40.9 MB | 2.7 MB | 27 KB (`_unresolved` 0.8 KB, `_seq` 0.4 KB, lanes 24 KB) | 43.6 MB | 575 B |
| synthetic, in memory | 1,000,000 | 471.3 MB | n/a | 18 KB | 471.3 MB | 471 B |
| synthetic, over NATS | 1,000,000 | see below | | | | |

Synthetic: 3 topics, 4 keys, 4 producers, 100-byte payloads, a pattern subscription and a topic subscription that stores
`position_of`, batches of 1,000 drained (a tick loop). Process RSS after the 1M in-memory run: 544 MB (grew by 527 MB).

Per structure: **the logs are the memory** (99.99 %): one `Envelope` (12 fields, an `msg_id` string, the payload) per message and
topic, about 400 to 470 bytes, plus on a `LogBus` one int in `_tokens` and a larger decoded envelope (575 B). Everything else is
small and bounded by names, not by messages: lanes and cursors (by distinct `(topic, key)` and topic per subscription: kilobytes),
routes, `_seq` (by producer), `_unresolved` receipts (resolved at the next read), position bookkeeping (`_Cursor.done` holds only
out-of-order offsets). Dead letters: nothing in these runs (they grow only with failures, see the design).

## 2. Design

### What holds memory and what may be trimmed

| Structure | Grows with | Decision |
|---|---|---|
| `_logs[topic]` (and `LogBus._tokens[topic]`) | messages | **retention** (below) |
| lanes `(topic, key)` of a consumer | distinct keys | freed when empty and idle (a lane is recreated by the next message; the owner is chosen again by the same stable hash) |
| `_Cursor.done` | out-of-order completions | already bounded (frontier advances) |
| `_dead` (dead letters) | failed deliveries | **not trimmed**: `dead_letters()[n:]` is how `PhaseDriver` and the participants find the new ones, so a bounded list would break the controller; a failing handler normally aborts a run. Documented as the one unbounded structure |
| heap, routes, `_seq`, `_groups`, receipts | topics, producers, unread publishes | bounded by names or by the read cadence |

### Retention of a log

An entry of a log serves three things only: replay `Start.EARLIEST`, replay from a `Position` (a stored checkpoint, a receipt) and the
resume position `position_of` hands out. It does **not** serve pending deliveries: a lane keeps its own references to the envelopes it has
not settled. So trimming a log never loses a delivery, and "what may be trimmed" is a question about replay only.

* `Retention(max_messages=N)` (constructor option `retention=` of `InMemoryBus` and `LogBus`; default `Retention()` = unlimited, today's
  behaviour, so every test that relies on unlimited replay is unchanged): per topic, at least the newest `N` entries are kept; older ones are
  dropped in batches (when more than `N + N // 4` are held), so a topic holds between `N` and `1.25 N` entries and the cost of trimming is
  amortised. It is a hard bound: memory is a function of `N`, not of the run's length, even with a stalled subscription.
* `bus.trim(position) -> Position`: the **explicit low-water mark**. The owner of a run advances it ("everything before this tick is
  checkpointed"): entries of `position.topic` below `position` are dropped, but **never past what a live subscription still needs**: the
  lowest frontier (`position_of` of its next delivery) of the subscriptions that have received the topic. Returns the oldest position
  now retained (a clamp is not an error: the caller sees how far it got). A position beyond the log's end or a topic without a log is a
  `ValueError`; trimming below what is already trimmed does nothing.
* `bus.oldest(topic) -> Position`: the earliest position that can still be replayed from.
* Offsets and positions stay absolute (a trimmed log keeps counting): `Receipt.position`, `Delivery.offset` and `position_of` mean the same
  thing before and after a trim.
* **A position older than the retained range fails clearly, never replays less**: `subscribe(start=Position)` with a position below
  `oldest(topic)` raises `PositionTooOld` (a `BusError`; carries `position` and `oldest`). `Start.EARLIEST` means "the whole log": once any
  matching topic has been trimmed it raises `PositionTooOld` too; a late subscriber that wants what is left subscribes at
  `bus.oldest(topic)`. `Start.LATEST`, and positions inside the range, are unaffected. `position_of` of a delivery whose frontier has
  already been trimmed returns that old position (resuming from it then fails clearly).
* Groups, request/reply inboxes, redelivery, dead letters: untouched (they never read the log after subscribing; an inbox topic is
  trimmed like any other).

### `LogBus` and NATS

* The mirror is the same structure and takes the same `retention=`; it is trimmed as entries are read (bounded also on the initial read of
  a long stream). `_tokens[topic]` is trimmed with its log. A position token below the topic's retained range is `PositionTooOld`.
* The stream has its own retention, which the mirror does not control: `JetStreamLog(max_messages=, max_age=, max_bytes=)` set the
  stream's limits (`LimitsPolicy`, discard oldest). A bus that attaches late to a stream the limits have trimmed sees that the first message
  it reads is not the log's first (`Log.first_position`, 1 for JetStream): positions and `EARLIEST` below it are `PositionTooOld`.
  `WorkQueue` and `Interest` retention are **not** usable: they delete a message once consumers acknowledged it, and this log's
  consumers read without acks, from the start, so every late attach and every replay would find the history gone.
* What else a run needs: the stream and the consumer are deleted at close (`drop_on_close`); a **crashed** process leaves both (the
  consumer expires after 24 h, the stream stays by design: it is the durable log), so a long-lived deployment sets `max_age`/`max_messages`
  and orphaned runs are removed with `jetstream.delete_stream(namespace)` (new, tested).

### Interface and wrappers

`Bus` gets `oldest(topic)` and `trim(position)`; `Retention` and `PositionTooOld` are exported. `SerializingBus` and `SeededFaultBus`
delegate both (the fault bus's restart resumes from a stored checkpoint: if the window has passed it, the restart fails with
`PositionTooOld`, which is the clear failure the design asks for).

### Tests

Contract (every bus: in-memory, serialising, NATS): a window drops the oldest; `oldest` advances; a trimmed position, and `EARLIEST` after
a trim, raise `PositionTooOld`; `oldest` as a start replays what is left in order; pending deliveries of a live subscription survive a
window that passed them; `trim` is clamped to the frontier of a live subscription and not past the log's end; the default keeps unlimited
replay. Memory-only: batches of the window, lane freeing, `retention` validation. NATS: the stream limits and a late attach, `delete_stream`.
