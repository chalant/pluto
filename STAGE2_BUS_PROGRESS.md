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
- [ ] 4. the NATS implementation
- [ ] 5. measurements, final run

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
