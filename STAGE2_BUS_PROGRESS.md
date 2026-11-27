# Stage 2, bus: the networked bus (NATS JetStream) (branch `stage2-bus`)

Base: `origin/master` = `dc3e9aa4`. Worktree `/home/yves/.cache/hades-pluto-dev/stage2-bus/wt`, env
`/home/yves/.cache/hades-pluto-dev/stage2-bus/env` (`dev/setup_env.sh`). Not pushed.

Decisions of the owner: NATS JetStream is the control bus; market data stays off the bus (a separate data plane, later).
The broker spike (`/home/yves/.cache/hades-pluto-dev/spike-broker/REPORT.md`, branch `spike-broker`) also has a Kafka log
(`spike/kafka_log.py`) that passes the same contract; Kafka is **not** in product code.
Out of scope: components in separate processes and their registry, the fault-injecting bus (another worker, another
`pluto/bus/` module), recovery by replay, log retention, the data plane.

## Status

- [x] 1. `Receipt.position` without a synchronous acknowledgement
- [x] 2. the controller waits for its expected dones
- [x] 3. explicit cross-producer dependencies inside a phase
- [x] 4. the NATS implementation
- [x] 5. measurements, final run

## Step 1: `Receipt.position` is resolved when asked for

Who used it: nothing in `pluto/` (only `Request.receipt` stores the receipt of a send); the contract and in-memory tests
read `receipt.position` (replay from a receipt, `Request.receipt`, topic of the receipt).

Change: `Receipt` is a small class (was a `NamedTuple`) with `msg_id`, `position` and `resolved`. The in-memory bus builds it
with a `Position`, as before (every existing test and caller is unchanged). A networked bus builds it with a function: `position`
calls it on first use and keeps the result (it waits for the broker's acknowledgement of that message), and the bus resolves the
receipts it holds when it reads the log. A message the broker refuses raises `BusError` from `position` or from the bus's next
read. `tests/test_bus_receipt.py` (4 tests). Contract assertions changed: none.

## Step 2: the controller waits for its expected dones, not for "idle"

Interface: the `Bus` protocol gets `networked` (True when another process writes the log this bus reads) and `poll(timeout)`
(read what other publishers appended, waiting up to `timeout` for the first message; True if any were read; never runs a
handler; False at once and `networked` False in memory). `InMemoryBus` and `SerializingBus` implement them. **Any other `Bus`
implementation must have both** (the fault-injecting bus another worker builds: inherit them from `InMemoryBus` or return
`False`).

`PhaseDriver.drive`: publish, then loop {`run_until_idle`; every expected done in? stop; a failure or a stale/doubled done?
stop (the run aborts); not `networked`? stop (nothing else can arrive, the missing done is an immediate `PhaseError`, as
before); deadline over? stop; `poll`}. The deadline is the controller's (`PhaseDriver(timeout=...)`,
`Controller(phase_timeout=...)`, default `phases.DEFAULT_TIMEOUT` = 300 s, `None` = none): `PhaseError ... no done from b
within 0.2 s` names who is missing. It is never used in memory. Tests: `tests/test_pluto_phases.py` (`NetworkedPhaseDriverTests`,
a bus that looks networked and delivers what a remote participant publishes one poll at a time; `InMemoryPhaseDriverTests`);
contract: 4 new tests for `networked`/`poll` (one skips on a bus that is not networked). Existing assertions changed: none.

## Step 3: what a run relies on between producers of one phase

Method: instead of only reading the code, `tests/test_pluto_phase_order.py` runs on buses that deliver in other legal orders
than S3: `ShuffledBus` (a seeded random order inside each `(logical_ts, phase)`; per-lane FIFO kept) and `ArrivalBus` (strictly
the order of publication). Whole runs (`buy_and_hold`, `cancel_orders`, `dividends`) and the fake-book tests of the strategy manager
with three runtimes (`tests/test_pluto_phase_participants.py` `RunTests`, now with a `make_bus` hook, run again on both buses)
give exactly the frame / the same calls. No result changed.

Cross-producer dependencies found (message of one producer needed by another handler) and how each is made explicit:

| # | Dependency | State before | Now |
|---|---|---|---|
| 1 | A runtime's account update needs the broker's state of the tick (the spike's worry: `strategy.account.<id>` from `broker` sorts before the controller's `phase.account` at the same `(ts, phase)` only by producer id) | already explicit: `RuntimeEndpoint` subscribes to its own `strategy.account.<id>` and ignores `phase.account`; no change | documented in `phases.py`; the shuffled and arrival runs prove it |
| 2 | The runtime's trade needs its account state (and the book written) | the controller publishes the next phase only after every expected done of the last: barrier | the barrier of step 2 (a log is a prefix: a reader that sees the event has seen everything published before its `done`); documented |
| 3 | Submit needs the batches of orders and the trade dones the broker side handled (`_sent`, `received_*`) | the counts in the trade done are checked at submit: a batch not handled is a `PhaseError` (loud), orders at `(ts,40)` < dones `(ts,41)` < submit `(ts,50)` | no change; same barrier; documented |
| 4 | The stale-batch filter of `StrategyManager.receive_orders` needs `_global_ts` set by the account handler | account barrier before trade | no change (a wrong filter shows in the counts of 3) |
| 5 | Order of the strategies in `new_orders` | rebuilt from the registration order (review fix H1) | tests of 3 runtimes run on both buses |
| 6 | `PhaseDriver` dones | collected and judged as a set, order-insensitive | step 2 waits for the set |

So no code had to change for this step: the dependencies were already explicit or protected by the barrier. What the step
adds is the test that proves it (two orderings, whole runs and multi-runtime) and the explanation in the module docstring of
`pluto/control/phases.py`. Not in scope and still open: the direct (non-bus) reads of the book by the controller's behavior and
the metrics tracker (`STAGE1_PHASES_PROGRESS.md`, "Left open").

### Review input of the fault-bus worker (applied to steps 2 and 3)

From `/home/yves/.cache/hades-pluto-dev/stage2-faultbus/wt/STAGE2_FAULTBUS_PROGRESS.md` ("where the phase protocol breaks"):
with a driver that waits for dones, a runtime's trade done can come before its order batch, and a late batch was dropped
silently by the stale filter. Changes (results unchanged, whole suite green):

| Item | Change |
|---|---|
| Submit must wait for the batches the dones announce | the **controller** waits (`PhaseDriver.announced_orders`): after the trade phase it reads the log (`poll`, deadline of the driver) until it has seen every announced batch and order count of the tick on `strategy.orders` (the driver subscribes to it), `PhaseError` naming the runtimes whose batch did not come (`no batch of orders from a within 300 s (a announced 1 orders in 1 batches, the log has 0 in 0)`). In memory nothing can arrive: it does not wait and the broker side reports the mismatch as before. Because a log is a prefix, the broker side then has every batch before it reads the submit event |
| The broker learns the counts from its own second subscription to the dones | gone: `BrokerParticipant` no longer subscribes to the trade dones. The counts travel **in the submit event** (new `SubmitEvent{clock, runtimes[RuntimeOrders{participant, orders, batches}]}` in `protos/phases.proto`, `phases_pb2.py` regenerated; `phases.message_types()` has `SubmitEvent`); the controller builds it from the dones it accepted |
| Broker side waits too | on a networked bus `BrokerParticipant` defers the submit while a runtime has sent fewer orders or batches than the event names (it goes on when `strategy.orders` brings them: a second, rank-1 subscription to that topic), and more than announced is an immediate error; in memory a mismatch is an immediate error, as before. Its own deadline is the controller's (the submit phase's missing done names the broker) |
| A late batch silently dropped | `StrategyManager.receive_orders` now counts a batch for an earlier or an already closed tick (`clear()` closes the tick: `_closed_ts`) instead of dropping it; `take_late_batches()`; the broker participant reports them (`aaa sent a batch of 1 orders for <tick>, which arrived after its tick closed`) in the submit phase of the tick and in the account phase of the next, as a `PhaseError` |
| Account state before trade must stay an explicit dependency | documented in `phases.py` (already) and now **enforced** in `RuntimeEndpoint`: it records the tick of the last broker state it applied and answers a `phase.trade` of another tick with a done carrying an error, without running the strategy |

Not done (next step by decision): idempotent handlers, deterministic `msg_id`s for retries and duplicates. No fault-injecting bus here.
Tests: `tests/test_pluto_phases.py` (3 networked `announced_orders` tests, 1 in-memory), `tests/test_pluto_phase_participants.py`
(`NetworkedSubmitTests` 4, `RuntimeEndpointTests` 1, one late-batch test in `RunTests`), the codec test has `SubmitEvent`.
Assertions changed: none (new tests only; `CodecTests` lists one more message).

## Step 4: the NATS implementation in product code

Files: `pluto/bus/log.py` (`Log` protocol, `LogBus`), `pluto/bus/jetstream.py` (`JetStreamLog`, `server_available`,
`DEFAULT_URL` = `PLUTO_NATS_URL` or `nats://127.0.0.1:4222`), `etc/requirements-py313.txt` pins `nats-py==2.17.0` (Apache-2.0,
pure Python, no dependencies). `pluto.bus` exports `Log` and `LogBus`; `jetstream` is imported explicitly (it needs `nats-py`).
The spike's Kafka log (`spike-broker` branch, `spike/kafka_log.py`, same two operations) is **not** in product code.

Design (from the spike, `/home/yves/.cache/hades-pluto-dev/spike-broker/REPORT.md`): the broker is a durable, totally ordered log
of one run (one stream per run, one subject per topic, the position is the stream sequence); the dispatcher of `InMemoryBus` runs
on the reader's side over a mirror of the log (`LogBus` subclasses it), so S3, rank, nack/redelivery, dead letters, groups and
positions are the in-memory ones and the contract suite passes unchanged. Differences, all in the docstring of `pluto/bus/log.py`:
`publish` sends at once and does not wait (step 1; the acknowledgements are collected at the next read, a refusal is a sticky
`BusError`), the bus reads its own messages back, `step` reads the log first when this bus published or has nothing to dispatch,
no `QueueFull`, groups in-process only, payload `bytes` (wrap in `SerializingBus`), each bus mirrors the whole stream.
`JetStreamLog`: an event loop thread (nats-py is asyncio only), a sender task that publishes in order with `publish_async`, a
durable pull consumer without acks, `fetch` with the server's no-wait batches (one request when this log's own acknowledged
messages are known to be there, `consumer_info` first otherwise), `poll(timeout)` = a fetch that waits.

Tests: `tests/test_bus_contract.py` is parameterised over `InMemoryBus`, `SerializingBus(InMemoryBus)` and
`SerializingBus(LogBus(JetStreamLog))` (a new stream per bus, deleted on close; skipped without a server); `tests/test_bus_jetstream.py`
(15: publish does not wait, resolution of positions, replay/resume on a new bus of the stream, refusal, close sends, `poll`, order of
S3 across four producer processes after a barrier, no server); `tests/test_pluto_jetstream_run.py` (`buy_and_hold`, `cancel_orders`,
`dividends` over NATS: frame equal to the in-memory run at rtol=0, no dead letters). Without a server every NATS test is skipped.

Contract suite changes (the suite is ours): **assertions changed: none**. Added: `test_poll_reads_the_log_and_never_runs_a_handler`,
`test_a_bus_nobody_else_writes_to_has_nothing_to_poll`, `test_poll_finds_a_message_published_by_another_bus_of_the_same_log`
(skips on a bus without a second writer: in memory and serialising), `test_poll_after_close_raises` (step 2); a third class
`JetStreamBusContract` with an `attach` hook; the factory `make_nats`; `scenario_trace` closes its bus before returning (so a
networked bus leaves no stream; the trace it returns is unchanged); `FACTORIES` has `"nats"` for the hash-seed test.

Found while testing (not bugs of the product): two buses on one stream must not assume what the other has published is in
their log yet (weak quiescence): the tests wait for what they expect, like the controller; one lane (topic, key) is FIFO in log
order, S3 only orders across lanes.

Revision after the first measurement (see step 5): the acknowledgement of a publish is now the message coming back. Each
message of a `JetStreamLog` carries a header with the log's id and its number; `read_new(wait, complete)` returns entries
`(position, topic, data, own)` and does not return before every message appended so far has been read back, in order, so one
request answers "did the broker take my messages" and "what is new", and `LogBus` resolves the receipts from the entries (the
`Log` protocol lost `flush`). A refusal (no stream) is caught from the ack futures by a done-callback and raised by the next read;
it is sticky (`LogBus._check_open`). `step` no longer reads the whole log when it has nothing to dispatch (that was a
`consumer_info` round trip per phase): idle means idle here; a caller that waits for others `poll`s (the `PhaseDriver` does),
`subscribe` still reads everything first.

## Step 5: measurements and the final run

Machine: 16 cores, shared (load average 10 to 21 during the runs), NATS server v2.15.3 on localhost with the file store; numbers are
indicative, the modes alternate so compare the pairs. `dev/bench_jetstream_run.py` (`pytest dev/bench_jetstream_run.py -s`) runs
`buy_and_hold` (7,312 ticks, 75,888 messages through the bus in this version: the metrics phase is gone since the spike) in
memory, over NATS with `publish` waiting for the broker (`WaitingLog`: each `append` reads the message back before it
returns: what a synchronous publish costs on the same transport), and over NATS as the bus is (publish does not wait).

| Run of `buy_and_hold` | in memory | NATS, publish waits | NATS, publish does not wait |
|---|---|---|---|
| spike (untuned, own adapter, 93,276 messages in that version) | 4.7 s | 78 s | n/a |
| step 1 as first built (ack by `flush`, then a read request: 2 round trips per step) | 4.6 / 3.6 s | 63.0 / 61.5 s | 58.0 / 58.1 s |
| as finished (ack is the read-back, one request per step, no idle `consumer_info`) | 4.4 / 3.7 s | 46.0 / 46.1 s | **35.2 / 37.3 s** |

So publishing without waiting is worth about 20 % (46 to 36 s) on the same transport, the single-request read about 40 % on top
(58 to 36 s), 78 to 36 s from the spike; per message 0.47 ms (36 s / 75,888) against 0.77 ms. What is left per step is one thread
hop plus one broker round trip (~270 us measured on the log alone) and the decode of the message; further gains (a push
consumer so that the answer is already there, fewer steps per phase) are not done. The whole-run test over NATS takes 42 s for
`buy_and_hold`, 8 s for `dividends`, 2 s for `cancel_orders` in the suite.

Full suite, fresh clone `/home/yves/.cache/hades-pluto-dev/stage2-bus/verify` of `stage2-bus` at `317ae25e` (code of the head), fresh env from
`dev/setup_env.sh`, `pytest tests`:

| | passed | skipped | time |
|---|---|---|---|
| NATS up | 581 | 2 (the poll tests that need a second writer, on the two in-memory buses) | 230.6 s |
| NATS stopped (`stop.sh`, restarted with `start.sh` after) | 476 | 107 (every NATS test: the NATS contract class, `test_bus_jetstream.py`, `test_pluto_jetstream_run.py`, each with "no NATS server at nats://127.0.0.1:4222") | 166.8 s |
| NATS up, `PYTHONHASHSEED=1` | 581 | 2 | 224.8 s |

The NATS tests add about 64 s (+38 %) to the suite; the whole-run test over NATS is 52 s of it. NATS is running at the end.
