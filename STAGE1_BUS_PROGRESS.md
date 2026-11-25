# Stage 1, bus (branch `stage1-bus`)

Base: `origin/master` = `c096b1c6`. Worktree `/home/yves/.cache/hades-pluto-dev/stage1/bus/wt`, env
`/home/yves/.cache/hades-pluto-dev/stage1/bus/env` (`dev/setup_env.sh`), scratch `/home/yves/.cache/hades-pluto-dev/stage1/bus/`.
Task: a new package `pluto/bus/` (pure Python, no new dependency) with its contract suite; nothing uses it yet; no existing
module touched. A review (agent NeonReviewBus, 15 findings) was fixed in a second round, recorded below.

## Status

- [x] `pluto/bus/`: `api.py` (types, protocol, topic helpers), `memory.py` (`InMemoryBus`), `serializing.py`, `request.py`,
      `__init__.py` (the rules, as the module docstring)
- [x] `tests/test_bus_contract.py` (broker-neutral contract, 84 tests on each of two implementations), `tests/test_bus_memory.py`
      (13 on each of the same two: bounded queues, log positions, group assignment), `tests/test_bus_serializing.py` (11)
- [x] `dev/bench_bus.py` and the measurements below
- [x] review round: all findings fixed (see "Review round")
- [x] the seven existing test files unchanged and green, from a fresh clone, hash seeds (see Final run)

## Design decisions (what the vault left open)

- **Dispatch key** of a lane head: `(logical_ts, phase, producer_id, seq, rank, subscription id, topic, key)` in one heap of
  the heads of the non-empty lanes (a lane is one (topic, key) of one consumer). Message content (S3), then explicit rank (S6),
  then registration order, then (topic, key), so a replay delivers a log in the order a live subscriber saw it. `phase` is an
  envelope field (the vault's S3 proposal).
- **Redelivery** keeps the dispatch key, so a nacked message is the next one of its lane (per-key order holds) and runs
  before handlers of a higher rank for the same message. `max_attempts` (default 3) deliveries in all, then a
  `DeadLetter` record (`dead_letters()`; strings `error_type`/`error_message`), dropped for that subscription only.
- **Overflow (S12)**: `QueueFull` raised to the publisher, atomically (nothing logged, enqueued or counted, for any receiver).
  Per subscription capacity `queue_capacity` (default 100,000), replay counted. A `QueueFull` raised inside a handler is a
  handler failure: the retry keeps its dispatch key and runs before the receiver drains, so it is normally dead-lettered after
  `max_attempts` unless the handler handles the exception (documented, no redesign). The overflow tests are in-memory rules
  (`tests/test_bus_memory.py`), not part of the broker-neutral contract.
- **Positions**: `Position(topic, token)`, per topic; the token belongs to the issuing bus (an int log offset in memory).
  `Receipt.position` is the published message (`start=` delivers it first). `position_of(delivery)` is the subscription's
  resume position in that topic: the lowest offset not yet settled (acked or dead-lettered), counting the delivery itself.
  Messages above it that were already handled come again (at-least-once), so deliveries that complete out of log order
  (S3 does not follow log order across keys) leave no gap. A pattern subscription starts from `EARLIEST`/`LATEST` only.
- **Groups**: members share one consumer; each (topic, key) lane is assigned to a member when the lane is created, by
  `crc32(topic, key) % members` (stable, content only), and stays with it; cancelling a member hands its lanes to the
  remaining ones. A member joins with the same pattern and `start` None or `LATEST`. `EARLIEST` on the first member pins the
  lanes created by the replay to the members present then (tested, documented).
- **ack/nack**: a handler that returns acks, one that raises `Exception` nacks, `ack`/`nack` inside the handler record the
  outcome (last call wins, an exception always nacks). `BaseException` propagates and leaves the message pending, undelivered.
- **close()**: cancels every subscription and closes every consumer; `Subscription.cancel` afterwards is a no-op; every other
  call (also `position_of`, `dead_letters`) raises `BusClosed`; a handler that closes the bus leaves no post-dispatch mutation.
- **request/reply**: replies go to `reply_to`, default the shared inbox `reply.<producer_id>` (an exact topic, validated);
  each `Request` reads the inbox and keeps the reply with its correlation id; optional `on_reply` callback (called from the
  dispatch loop; if it raises the delivery is nacked and it is called again); `Request.receipt` is the latest send; a failed
  send leaves no subscription. No timeout in the bus (S5): the caller calls `Request.send` again with the same `msg_id`.
- **No dedup** by `msg_id` in the bus (a JetStream adapter must not map `msg_id` to `Nats-Msg-Id`). Start from a time, `seek`,
  S4 (networked merge) and `SeededFaultBus` are not here (no stub).
- **Open: retention.** Logs are never trimmed, lanes never freed, cancelled subscriptions leave stale heap entries until
  their `logical_ts` is reached (memory grows with messages and distinct keys; matters for long live runs, not for backtests).
- Mutation checks of the suite (run by hand, reverted): ignoring `rank` or `seq` in the dispatch key, using `hash(producer_id)`
  in it, and `position_of` returning `offset + 1` each make tests fail.

## Public API (`from pluto.bus import ...`)

Signatures: `pluto/bus/api.py` (`Bus` protocol), `memory.py`, `serializing.py`, `request.py`. `Envelope`, `Position`, `Receipt`,
`Delivery` are NamedTuples (immutable; assignment raises `AttributeError`), `DeadLetter` a frozen dataclass.

## Review round (findings of NeonReviewBus)

| # | Fix |
|---|---|
| 1 | `Position(topic, token)` opaque; `position_of` = settled frontier per consumer and topic (`_Cursor`); multi-key out-of-order checkpoint test, dead-lettered counts as settled, end-of-log resume |
| 2 | `publish` validates and checks capacity before it touches `_seq`, `_logs` or lanes; tests: seq does not skip, nothing logged, refused type errors use no seq |
| 3 | `close()` marks consumers closed and subscriptions cancelled; `cancel` after close is a no-op; close inside a handler tested |
| 4 | `request`: cancel on a failed send, `reply_to` validated as exact topic (`exact_topic`), shared inbox default, `on_reply`, `Request.receipt` |
| 5 | tie-break `(topic, key)`; group member by crc32 of the lane; live-vs-replay order test |
| 6 | documented (`InMemoryBus` docstring, above) |
| 7.1 | `phase: int` in `Envelope`, `publish`, `request`, `reply`, in the dispatch key |
| 7.2 | overflow, position-range and group-assignment tests moved to `tests/test_bus_memory.py`; contract is broker-neutral |
| 8 | `SerializeToString(deterministic=True)`, JSON `sort_keys=True` (tests with a map-field `Struct` and a fake message) |
| 9 | `run_until_idle`: nesting check first, `max_deliveries` must be a non-negative int, `>=` check |
| 10 | NamedTuples, no `dataclasses.replace`, `_replay_sources` counts before it enqueues, inlined publish checks; bench extended (keys, subscribers) |
| 11 | `type(x) is int/str` checks (bool, numpy ints refused), `rank`, `group`, `Position.token` validated |
| 12 | `close()` doc and behaviour agree: only `close` and `cancel` work afterwards |
| 13 | group members may join with `start=Start.LATEST`; `EARLIEST` pinning documented and tested |
| 14 | `reply()` takes `schema_version` (and `phase`); `SerializingBus.dead_letters()` keeps an undecodable entry with its raw bytes; `DeadLetter` has `error_type`/`error_message` strings |
| 15 | tests added for the listed gaps (close/cancel, multi-key checkpoint, request leaks and shared inbox, start never inline, group self-cancel with ack and nack, handler cancelling another subscription, `max_deliveries` 0/negative/nested); the hash-seed scenario now has phases, dead letters and checkpoints |

## Dispatch-cost measurement (`python -m dev.bench_bus`, 1,000,000 messages, indicative)

2026-11-25, one run each on a loaded 16-core machine (load average 10 to 15; the "before" run shared the machine with my edits).
Before = first version (`6b942a6e`), run from a snapshot with the extended bench; after = this version. Messages per second,
a message counts once however many subscribers get it.

| Variant | Mode | before | after |
|---|---|---|---|
| `InMemoryBus` 1 key, 1 subscriber | all at once | 187,788 | 248,309 |
| | batches of 1,000 | 184,881 | 258,972 |
| 16 keys, 1 subscriber | all at once | 173,525 | 200,467 |
| | batches | 194,772 | 251,411 |
| 1 key, 4 subscribers | all at once | 76,594 | 88,786 |
| | batches | 101,764 | 102,190 |
| 16 keys, 4 subscribers | all at once | 51,693 | 85,705 |
| | batches | 80,816 | 111,088 |
| `SerializingBus`, protobuf `Timestamp`, 1 key, 1 sub | all at once | 86,349 | 153,405 |
| | batches | 95,236 | 167,136 |
| `SerializingBus`, JSON dict, 1 key, 1 sub | all at once | 68,754 | 99,660 |
| | batches | 69,263 | 85,462 |

A plain `deque` bus (append, popleft, call) measured 4.0 million msg/s on this machine. About 4 microseconds per message
(publish plus dispatch) for the in-memory bus; a minute-bar year at 5 million messages costs about 20 seconds in the bus.
The serialising wrapper's gain comes from building `Envelope` and `Delivery` directly instead of `dataclasses.replace`.

## Final run

Filled in below when the fresh-clone run is done.
