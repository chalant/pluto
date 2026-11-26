# METRICS_FOLD_PROGRESS: metrics from the event stream of the book

Branch `metrics-fold` (from `origin/master` `6a6941d6`). Plan: "Metrics Separation" (the loop emits events, a separate consumer
computes the metrics), as mapped in the report `~/.cache/hades-pluto-dev/metrics-map/REPORT.md` (read-only investigation).
Rule of the work: no result changes (Pluto equals Zipline at `rtol=0, atol=0`; the stored expected results are untouched; the
original test files `test_pluto_examples`, `_zipline_oracle`, `_isolation`, `_assets`, `_scenarios`, `_state`, `_upstream_fixes`
do not change).

Baseline on `origin/master` (`6a6941d6`), whole suite `pytest -p no:randomly -q tests`: **374 passed, 6 subtests passed** (120 s).

## Map (what the investigation found, short)

- The `metrics` phase (`MetricsParticipant`) read the book's ledger object at SESSION_START, MINUTE_END and SESSION_END and called
  the harness tracker (`test_utils.TestMetricsTracker`) with it. Daily runs only have SESSION_START and SESSION_END: two reads
  per session. The portfolio is revalued lazily and each revaluation compounds `returns` and `pnl`, so the book revalues
  (`BrokerService.revalue`, `Revalued` on the stream) right before each read: one extra revalue at TRADE_END changes 9 of the
  26 frames (examples + scenarios) at rtol=0.
- The 26 runs are the 5 examples and the 21 scenarios (the older progress files said 23 scenarios: corrected).
- The strategy runtime has its own ledger and tracker (its `context.portfolio` / `context.account`) and also computed a
  performance packet at each `session_end` / `minute_end` that nothing consumed on the simulation path (`NullPerformanceWriter`
  without observer; `Process.watch` has no caller). It did not even equal the controller side (10 of 26 runs differ in `returns`
  by an ulp or in the trigger flags of the orders).
- The stream could rebuild the book (fold) but did not say where a reader may read.

## Step 1: observation marker and a stream consumer, beside the metrics phase

Done (commit "Metrics from the event stream: observation marker and a stream consumer").

- `events.Observed(clock_event, dt)`: published by `BrokerService.observe(calendar, clock_event, dt)`, called by the book phase as
  its last call for the SESSION_START, MINUTE_END and SESSION_END signals (after `revalue`). It writes nothing; `fold` advances
  past it (`stream._apply`). The marker takes a sequence number like any event (the saved `stream_sequence` counts it).
- `pluto/finance/metrics/consumer.py`: `StreamMetrics(start_dt, capital, tracker)`, a subscriber of the broker service: it folds
  each event into its own `Replica` and calls `start_of_session` / `end_of_bar` / `end_of_session` of the tracker with the
  replica's ledger at each marker. A failure of the tracker or of the fold is caught at the consumer's boundary and recorded in
  `consumer.error` / `consumer.error_sequence`; the consumer then ignores later events (its copy is not known to be right).
  The book's write that published the event always completes.
- `tests/test_pluto_metrics_stream.py`: on all 26 runs the frame of a second harness tracker fed by the consumer equals the frame
  of the tracker of the metrics phase (length, index, column set, `frame_errors == []` at rtol=0, atol=0); negative test: a
  consumer that revalues its copy once more (after each `PricesMarked`) is not equal on `market_orders`; marker position test;
  unit tests of `observe`, reads of the copy, a failing tracker not reaching the book's write, unknown clock event, stream gap.
- `tests/test_pluto_phase_participants.py`: `FakeBroker` got an `observe` method that does nothing (a test double of the new
  service method, no assertion changed).
- Docs: scenario count 23 -> 21 (and 28 -> 26) in `STAGE1_ENGINE_PROGRESS.md`; `dev/README.md` test table.

Measured: the new file 33 tests pass; with the broker service, phase participants and phase run tests 108 passed.

## Step 3: the runtime's performance packet is gone

Done (commit "Delete the runtime's performance packet computation"). The packet was computed at every `session_end` /
`minute_end` of the strategy runtime and dropped (nothing consumed it on the simulation path, and it did not equal the
controller side). The runtime keeps its book (`Ledger` + `MetricsTracker` behind `context.portfolio` / `context.account`).

Removed:
- `MetricsTracker.handle_market_close`, `handle_minute_close`, `_execution_open_and_close`, the `pluto_metrics()` instances and
  their loops (`pluto/finance/metrics/tracker.py`); `handle_market_open` now only starts the session of the ledger (its
  `data_portal` argument stays: `test_pluto_state.py` passes it; it is documented as unused).
- `Controllable._get_daily_message`, `_get_minute_message`; `session_end(dt)` and `minute_end(dt)` return the `end` flag only (it
  still drives `controllable.ended`, `commands.ClockUpdate`).
- `commands.ClockUpdate` takes no performance writer.
- The writer chain, which had no producer left and no other user (checked with grep over `pluto/`, `tests/`, `dev/`):
  `pluto/control/controllable/performance_logging.py` (`PerformanceWriter`, `NullPerformanceWriter`, `Observer`, `read_perf`,
  `write_perf`); in `server.py` the `monitor` constructor argument, `log_performance` (property and `-lp` option), the performance
  file and `Watch` / `StopWatching`; `ControllableStub.Watch/StopWatching` and their in-memory implementations;
  `InMemoryMonitorStub`; `StrategyStub.watch/stop_watching` and its `monitor.<id>` event; `ProcessFactory.set_monitor_service`
  (no caller); `pluto/interface/monitor.py` (`Monitor`, `GeneratorWatcher`, `AbstractWatcher`: only `in_memory.py` imported it).

Kept, and why:
- `conversions.to_proto_performance_packet` / `from_proto_performance_packet` and the `controller.proto`, `metrics.proto`,
  `monitor.proto`, `interface.proto` definitions: `tests/test_pluto_state.py` (`PerformancePacketTest`) uses the conversions and the
  original tests must not change; `pluto/coms/controller.py` (client of the controller service) uses
  `from_proto_performance_packet`; the `.proto` files are the interface definitions.
- `RunSetup.clear_perf` and the field of the Run request: `tests/test_pluto_examples.py` sets it. The server no longer reads it.
- The `performance` directory of the strategies directory (`directory_setup.py`): layout of the directory, unused now.
- `MetricsTracker(ledger, capital, data_frequency, emission_rate, start_dt)` signature, `get_state` / `restore_state`,
  `process_*`, `.ledger/.portfolio/.account/.positions`: `test_pluto_state.py` and the runtime's use.

Measured: full suite on the tree after step 3: **407 passed, 6 subtests passed** (374 on master + the 33 tests of
`test_pluto_metrics_stream.py`); the equality files, state and isolation: 33 passed.

## Step 2: the harness reads the stream, the metrics phase is gone

Done (commit "Metrics are a consumer of the stream: delete the metrics phase").

- `StrategyTester.run` (`pluto/test_utils/test_utils.py`) builds `TestMetricsTracker`, a `StreamMetrics` consumer over it
  (`BrokerService.start_dt` / `capital` are now read-only properties of the service) subscribed to `manager.broker`, and hands the
  consumer to the controller. `_run(...).frame()` of the original tests is unchanged: the frames now come from the stream.
- `Controller(strategy_manager, market, behavior, metrics=None)`: no `metrics_tracker` parameter, no `METRICS` drive. After the
  book phase of each tick (all signals) it calls `metrics.raise_if_failed()`: `consumer.MetricsError`
  ("the metrics consumer failed at event N of the stream of the book: ExcType: message", `__cause__` = the tracker's exception).
- Deleted: `phases.METRICS`, `phases.METRICS_ID` (the ranks of the other phases are unchanged: 10, 30, 40, 50),
  `participants.MetricsParticipant`, `participants.MetricsTracker` (the consumer has its own `Tracker` protocol),
  the controller's direct read of the ledger (`get_ledger` has no production caller now; it stays for
  `tests/test_pluto_broker_service.py`).
- Tests of the deleted phase, rewritten (the original equality files are untouched). Every removed or changed assertion:
  - `tests/test_pluto_phase_participants.py`: `FakeTracker` removed; `FakeBroker.observe` now records `('observe', calendar,
    clock_event, dt)`; `RunTests.setUp` builds `Controller(manager, None, FakeBehavior(), consumer)` with a real
    `StreamMetrics`; `test_a_tick_with_two_signals_interleaves_the_book_and_the_metrics_per_signal` is now
    `..._runs_the_book_phase_per_signal` and expects `('observe','XNYS',MINUTE_END,TS)` where it expected
    `('end_of_bar','XNYS',TS)` and `('observe','XLON',SESSION_END,TS)` where it expected `('end_of_session','XLON',TS)`;
    `ParticipantTests` has no metrics participant and `run_signal` drives the book phase only; in
    `test_the_calls_of_each_clock_event` the tracker reads `end_of_bar`, `start_of_session`, `end_of_session` of MINUTE_END,
    SESSION_START, SESSION_END are replaced by the `observe` call of the same events (the other three events are unchanged).
    New: `test_a_failed_metrics_consumer_stops_the_run_after_the_book_phase_of_the_tick` (the book calls are all there, the
    strategy did not run, nothing submitted).
  - `tests/test_pluto_phases.py`: `test_dones_of_a_phase_do_not_leak_into_the_next` drives `ACCOUNT` where it drove `METRICS`
    (same assertion); `test_phases_are_in_the_order_of_the_calls` has no `METRICS` in its list (same assertions: ranks
    increasing, a gap above `DONE_AFTER`). `test_nobody_listening_aborts` still names participants `'metrics', 'broker'`: they
    are only labels there, left as they were.
  - `tests/test_pluto_phases_run.py` `test_what_a_run_publishes`: removed `book count == metrics count` and `metrics done count ==
    metrics count`; added: no topic of the run has `metrics` in its name.
  - `tests/test_pluto_metrics_stream.py` (mine, from step 1): the 26 "equal to the metrics phase's frame" tests became "computed
    from the whole stream" tests (no gap, the consumer's copy has the book's saved state); the old path no longer exists. The
    frames are still compared with Zipline and with the stored results by the original files, which now run through the
    consumer. Kept: the extra-read negative test, the observation point test, the unit tests. New: a failing tracker mid-run
    stops the run at its tick with `MetricsError`, the book complete.

Measured on one `buy_and_hold` run (plain `InMemoryBus`, `exp/measure.py` counting `publish` and the deliveries returned by
`run_until_idle`): master 85,968 messages / 93,280 deliveries (matches the figures of `STAGE1_PHASES_PROGRESS.md`); after step
2 **75,888 messages / 83,200 deliveries** (-10,080 each: the 5,040 `phase.metrics` events and their 5,040 dones, 11.7 % of the
messages). The book phase publishes the marker as an event of the stream (a callback, not a bus message).

Measured: full suite on the tree after step 2: **409 passed, 6 subtests passed** (374 on master; +34 tests in
`test_pluto_metrics_stream.py`, +1 in `test_pluto_phase_participants.py`).

## Failure policy of the metrics consumer (owner decision, 2026-11-26)

A metrics consumer error never interrupts a book write (implemented in the consumer). Then:
- **simulated run (backtest): the run stops with a clear metrics error** (implemented in step 2: the controller checks the
  consumer's recorded error after the book phase of each tick and raises `MetricsError`; the owner's wording was "at the end
  of the tick"; it is the first place after the tick's writes of the book where a check is possible, and the strategies do not
  run that tick);
- **live: trading continues, an alert is raised and the consumer restarts and replays from its last position.** Live is not
  supported in stage 1; this is the target behaviour, nothing of it is implemented. (The stream's sequence numbers, the
  idempotent fold and the saved `stream_sequence` are what a replay from the last position builds on.)

## Open items (not done, from the map)

- The stream has no initial-book event: the consumer takes `start_dt` and `capital` from the service's properties, out of band.
- `PricesMarked` carries no price rule/source (it is implicit in `SimulationControllerBehavior.get_last_sale_prices`).
- No event for capital changes, and no channel for recorded variables (`record()`); the benchmark and the empyrical columns
  are still computed in the harness class `TestMetricsTracker`, not in product code.
- Minute frequency and several calendars are not exercised by any test (the bundle runs are daily); `TestMetricsTracker`
  builds a daily row at each MINUTE_END.
- Live behaviour of a consumer error (see above) is a target only.
- After the clock is exhausted the controller keeps publishing the last SESSION_END signal (4 extra book phases on
  `buy_and_hold`); the harness tracker ignores them past the number of sessions.

## Verification

Fresh clone of branch `metrics-fold` at `c21cf93d` (code identical to the final head: the later commit changes only this
file), fresh environment (`dev/setup_env.sh`), `pytest -p no:randomly -q tests`: **409 passed, 6 subtests passed** (140.7 s),
and the same under `PYTHONHASHSEED=1`: **409 passed, 6 subtests passed** (134.9 s). Baseline `origin/master` (`6a6941d6`): 374
passed, 6 subtests passed (120 s). The seven original test files (`test_pluto_examples`, `_zipline_oracle`, `_isolation`,
`_assets`, `_scenarios`, `_state`, `_upstream_fixes`) have an empty diff against `6a6941d6`; Pluto equals Zipline at rtol=0,
atol=0 (the oracle and scenario tests pass unchanged, through the stream consumer).

## Review fixes (read-only review of the branch; no High issue)

### 1. The runtime ledger is revalued again at the end of a session and of a minute

The deleted `handle_market_close` / `handle_minute_close` read `ledger.portfolio`, which revalued the runtime's ledger
(dirty after every account update). Order in the old `session_end`: expired assets, the daily cancel policy, the account
controls, then the perf packet (`git show 6a6941d6:pluto/finance/metrics/tracker.py`: `first_session`, then the metrics loop,
the first read of the portfolio revalues). Restored: `Controllable.session_end` calls `self._metrics_tracker.ledger.revalue()`
after `algo.validate_account_controls()`, and `minute_end` calls it, as the packet did.

Tests (`tests/test_pluto_runtime_portfolio.py`, strategy in `tests/runtime_portfolio_strategy.py`, reference in
`tests/resources/runtime_portfolio_records.json`):
- `test_the_runtime_ledger_is_up_to_date_at_the_end_of_a_session`: in a real run (60 sessions), the runtime ledger is not dirty
  after any `session_end`. **Fails without the fix** (checked by reverting the change).
- `MinuteEndTests.test_minute_end_revalues_the_runtime_ledger`: `minute_end` on a dirty ledger leaves it clean (the bundle runs
  are daily, so a unit test with a real `Ledger`). **Fails without the fix** (checked).
- `test_the_strategy_sees_the_portfolio_it_saw_before`: the strategy reads `context.portfolio` / `context.account` every fourth
  day (the days in between are revalued by the runtime itself) and the 15 records (returns, pnl, portfolio value, cash,
  positions value, leverages) equal bit for bit the ones captured on `6a6941d6` (the code path before step 3, run in a scratch
  tree built from `git archive`). Honest limit: **this one does not fail without the fix** (the same records with and without
  it): in daily runs `Ledger.start_of_session` reads the portfolio at every session start, and the recompute at the end of the
  session leaves the same state, so the strategy-visible sequence does not change; the revalue point matters for the dirty state
  and for minute runs, which no test bundle run covers. It guards the strategy-visible values of step 3 as a whole.
- Not used as reference: Zipline. In a first version of the strategy (reading every day) the Pluto runtime's records differed
  from plain Zipline's from the second record on (pnl -4.0 in Pluto, -4.0000003264 in Zipline), and Pluto's records are the
  same on master (they equal the `6a6941d6` run): a difference of the runtime's view that exists on master; not looked into
  (open item).

### 2, 3, 6. The consumer and the harness

- `StreamMetrics.__call__` returns early for an event whose sequence is not above the copy's (`replica.sequence`): an event
  delivered twice was folded once (the fold skips it) but its `Observed` read the tracker twice. Test:
  `StreamMetricsUnitTests.test_an_event_delivered_twice_is_folded_and_read_once` (fails without the change: two reads).
- `StrategyTester.run` calls `metrics.raise_if_failed()` after the loop (an error of the last tick's submit phase, after the
  controller's check of that tick, was never raised). Tests: `HarnessTests` (a loop without ticks and a consumer that failed:
  `MetricsError`; fails without the change; and a run without error returns the tracker).
- `StreamMetrics.replica` is documented as for tests, to be read at observation points only.

### 4. Stale text

Docstrings of `BrokerService` (module, `get_ledger`, `revalue`) and of `BrokerParticipant._book` no longer say that the metrics
tracker reads the ledger; the comment at `tests/test_pluto_broker_service.py:65`; `dev/README.md` (the `performance` directory
is created and nothing writes to it); `pluto/api.pyi` `record()`: recorded variables are not emitted yet. Text only.
