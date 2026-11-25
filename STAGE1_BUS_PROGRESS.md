# Stage 1, bus (branch `stage1-bus`)

Base: `origin/master` = `c096b1c6`. Worktree `/home/yves/.cache/hades-pluto-dev/stage1/bus/wt`, env
`/home/yves/.cache/hades-pluto-dev/stage1/bus/env` (`dev/setup_env.sh`), scratch `/home/yves/.cache/hades-pluto-dev/stage1/bus/`.
Task: a new package `pluto/bus/` (pure Python, no new dependency) with its contract suite; nothing uses it yet; no existing
module touched.

## Status

- [x] `pluto/bus/`: `api.py` (types, protocol), `memory.py` (`InMemoryBus`), `serializing.py`, `request.py`, `__init__.py`
      (the rules, as the module docstring)
- [x] `tests/test_bus_contract.py` (contract suite, 66 tests per implementation, two implementations: 132) and
      `tests/test_bus_serializing.py` (7)
- [x] `dev/bench_bus.py` and the measurement below
- [x] the seven existing test files unchanged and green, from a fresh clone, hash seeds (see Final run)

## Design decisions (what the vault left open)

- **Dispatch key** of a lane head: `(logical_ts, producer_id, seq, rank, subscription id, lane index)` in one heap of the
  heads of the non-empty lanes (a lane is one (topic, key) of one consumer). The first three fields are message content
  (S3), then explicit rank (S6), then registration order. `phase` of the vault table is not an envelope field; a caller
  that needs a phase order encodes it in `logical_ts` or the producer id.
- **Redelivery** keeps the dispatch key, so a nacked message is the next one of its lane (per-key order holds) and runs
  before handlers of a higher rank for the same message. `max_attempts` (default 3) deliveries in all, then a
  `DeadLetter` record (`dead_letters()`), the message is dropped for that subscription only.
- **Overflow (S12)**: the vault says "block the producer in simulation"; blocking cannot work on one thread without
  running handlers inside `publish` (S2), so the policy is: `QueueFull` raised to the publisher, atomically (nothing
  logged or enqueued, for any receiver). Per subscription capacity `queue_capacity` (default 100,000), replay counted.
  A handler that hits it is nacked like any failure. A replay larger than the capacity is refused at `subscribe`.
- **Positions**: `Position(topic, offset)`, per topic (a pattern subscription starts from `EARLIEST`/`LATEST` only).
  `Receipt.position` is the published message (`start=` delivers it first); `position_of(delivery)` is the position after the
  delivered message (the checkpoint to store; `start=` resumes with the next message), as Kafka's committed offset.
- **Groups**: members share one consumer; each (topic, key) lane is assigned to a member at creation
  (`lane index % members`) and stays with it; cancelling a member hands its lanes to the remaining ones.
  A member joins with the same pattern and no `start`.
- **ack/nack**: a handler that returns acks, one that raises `Exception` nacks, `ack`/`nack` inside the handler record the
  outcome (last call wins, an exception always nacks). `BaseException` propagates and leaves the message pending, undelivered.
- **No dedup** by `msg_id` in the bus (the survey's "idempotent on msg_id within a window" is not implemented; the contract
  test says the bus redelivers). **No timeout** in `request` (S5): the caller calls `Request.send` again with the same
  `msg_id`. Start from a time, `seek`, S4 (networked merge) and `SeededFaultBus` are not here (no stub).
- Logs are never trimmed and lanes never freed (memory grows with messages and distinct keys).
- Mutation checks of the suite (run by hand, reverted): ignoring `rank` in the dispatch key, ignoring `seq`, and using
  `hash(producer_id)` in the key each make tests fail (the last one only the `PYTHONHASHSEED` tests).

## Public API (`from pluto.bus import ...`)

See the final report of the agent; the signatures are in `pluto/bus/api.py` (`Bus` protocol) and `memory.py`.

## Dispatch-cost measurement (`python -m dev.bench_bus`, 1,000,000 messages, one subscriber, indicative)

2026-11-25, load average about 16 on 16 cores (not quiet), one run. A plain `deque` bus (append, popleft, call) measured the same
minute at 4.0 million msg/s.

| Variant | Mode | msg/s total | publish /s | dispatch /s |
|---|---|---|---|---|
| `InMemoryBus` | all published, then drained | 216,632 | 287,652 | 877,426 |
| `InMemoryBus` | batches of 1,000 | 224,365 | 298,611 | 902,379 |
| `SerializingBus`, protobuf `Timestamp` | all at once | 89,650 | 216,032 | 153,245 |
| `SerializingBus`, protobuf `Timestamp` | batches | 94,983 | 246,986 | 154,337 |
| `SerializingBus`, JSON dict | all at once | 68,242 | 148,744 | 126,093 |
| `SerializingBus`, JSON dict | batches | 60,810 | 137,070 | 109,301 |

About 4.6 microseconds per message (publish plus dispatch) for the in-memory bus: envelope, receipt and delivery objects, the
log, the lane, the heap entry. At that rate a minute-bar year at 5 million messages costs about 25 seconds in the bus. If that
matters at stage 2, the first candidates are publish (3.5 microseconds of it) and the per-delivery object.

## Final run

Fresh clone of `stage1-bus` at `79e21e8d` (clone with `--no-hardlinks`, new env from `dev/setup_env.sh`), the seven existing
files plus `tests/test_bus_contract.py` and `tests/test_bus_serializing.py`: **210 passed** (71 existing, unchanged, 132
contract, 7 serializing), also under `PYTHONHASHSEED` 1, 2 and 3 (210 passed each). The contract suite itself starts child
interpreters under `PYTHONHASHSEED` 1 to 5 and compares the delivery trace of a 200-message scenario (wildcards, ranks, a
group, failures, cascades, a late replay). The verify clone was removed.
