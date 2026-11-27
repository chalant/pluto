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
- [ ] 3. explicit cross-producer dependencies inside a phase
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
