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

## Failure policy of the metrics consumer (owner decision, 2026-11-26)

A metrics consumer error never interrupts a book write (implemented in the consumer). Then:
- **simulated run (backtest): the run stops at the end of the tick with a clear metrics error** (implemented in step 2);
- **live: trading continues, an alert is raised and the consumer restarts and replays from its last position.** Live is not
  supported in stage 1; this is the target behaviour, nothing of it is implemented. (The stream's sequence numbers, the
  idempotent fold and the saved `stream_sequence` are what a replay from the last position builds on.)
