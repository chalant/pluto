"""The metrics are computed from the event stream of the book.

``pluto.finance.metrics.consumer.StreamMetrics`` subscribes to the broker service, folds the stream into its own copy of
the book and reads it at the observation points (``events.Observed``): the harness (``StrategyTester.run``) feeds its
tracker (``test_utils.TestMetricsTracker``) this way, and the equality tests (examples, oracle, scenarios) compare what
it computes with Zipline's and with the stored results, exactly. Here: every run of the 5 examples and 21 scenarios is
computed from its whole stream; a consumer that reads once more than the book revalued does not give the same frame; the
observation points of the stream; a failing tracker stops a simulated run at its tick with a metrics error, the book
complete; the consumer's unit tests.

Before the metrics phase was deleted, the frames of the consumer and of the phase's tracker (which read the book's ledger)
were asserted identical on all 26 runs, rtol=0, atol=0 (see METRICS_FOLD_PROGRESS.md).
"""
import itertools
import types
import unittest
from unittest import mock

import pandas as pd

from pluto import examples
from pluto.broker_service import events, service, stream
from pluto.finance.metrics import consumer as metrics_consumer
from pluto.test_utils import test_utils

from protos.clock_pb2 import MINUTE_END, SESSION_END, SESSION_START

from tests.pluto_scenarios import SCENARIO_MODULES
from tests.test_pluto_examples import _run
from tests.test_pluto_zipline_oracle import ZiplinePlutoFixture

START = pd.Timestamp('2014-01-02', tz='UTC')
DT = pd.Timestamp('2014-01-03 21:00', tz='UTC')
CAPITAL = 100000.0

ALL_MODULES = dict(examples.EXAMPLE_MODULES)
ALL_MODULES.update(SCENARIO_MODULES)
assert len(ALL_MODULES) == len(examples.EXAMPLE_MODULES) + len(SCENARIO_MODULES) == 26

RUNS = itertools.count()


class _RecordingService(service.BrokerService):
    """The broker service of a run, with its stream kept."""

    created = []

    def __init__(self, *args):
        super().__init__(*args)
        self.stream = stream.InMemoryStream()
        self.subscribe(self.stream)

        self.created.append(self)


class _RecordedConsumer(metrics_consumer.StreamMetrics):
    """The consumer the harness builds for the run, kept."""

    created = []

    def __init__(self, *args):
        super().__init__(*args)
        self.created.append(self)


class _ExtraRevalue(metrics_consumer.StreamMetrics):
    """Reads (revalues) its copy of the book once more than the book did: after each mark of prices."""

    def __call__(self, stream_event):
        super().__call__(stream_event)

        if self.error is None and isinstance(stream_event.event, events.PricesMarked):
            self.replica.ledgers[stream_event.calendar_name].revalue()


class _Run(object):
    """What a run gave: the frame of the harness (computed by its consumer), the consumer, the frame of a consumer that
    reads once more than the book revalued, the broker service with its stream."""

    def __init__(self, frame, consumer, extra_frame, broker):
        self.frame = frame
        self.consumer = consumer
        self.extra_frame = extra_frame
        self.broker = broker


def run_with_consumers(pluto_tempdir, name, environ, modules=ALL_MODULES):
    """Runs ``name`` through the controller's path, with the consumer of the harness kept and a second consumer, the
    extra-revalue one, subscribed next to it (its tracker is built like the harness one, with the same arguments)."""
    _RecordingService.created.clear()
    _RecordedConsumer.created.clear()
    extras = []
    original_init = test_utils.TestMetricsTracker.__init__

    def init(self, *args, **kwargs):
        original_init(self, *args, **kwargs)

        [broker] = _RecordingService.created
        twin = test_utils.TestMetricsTracker.__new__(test_utils.TestMetricsTracker)
        original_init(twin, *args, **kwargs)

        extra = _ExtraRevalue(broker.start_dt, broker.capital, twin)
        broker.subscribe(extra)
        extras.append(twin)

    with mock.patch.object(service, 'BrokerService', _RecordingService), \
            mock.patch.object(metrics_consumer, 'StreamMetrics', _RecordedConsumer), \
            mock.patch.object(test_utils.TestMetricsTracker, '__init__', init):
        tracker = _run(pluto_tempdir, name, environ, modules=modules)

    [broker] = _RecordingService.created
    [consumer] = _RecordedConsumer.created
    [extra] = extras
    return _Run(tracker.frame(), consumer, extra.frame(), broker)


class StreamMetricsRunTests(ZiplinePlutoFixture):
    _runs = {}

    def _run(self, name):
        runs = type(self)._runs

        if name not in runs:
            runs[name] = run_with_consumers(self._pluto_tempdir, name, self._environ)

        return runs[name]

    def check_computed_from_the_stream(self, name):
        run = self._run(name)

        self.assertIsNone(run.consumer.error)
        self.assertGreater(len(run.frame), 0)

        # the consumer folded the whole stream of the book, no gap
        stream_events = list(run.broker.stream)
        self.assertGreater(len(stream_events), 0)
        self.assertEqual([s.sequence for s in stream_events], list(range(1, len(stream_events) + 1)))
        self.assertEqual(run.consumer.replica.sequence, len(stream_events))

        # and its copy of the book is the book: the same saved state
        broker_ledger = run.broker.get_ledger('XNYS')
        last = run.frame['period_close'].iloc[-1]
        self.assertEqual(run.consumer.replica.ledgers['XNYS'].get_state(last), broker_ledger.get_state(last))

    def test_a_read_more_than_the_book_revalued_is_not_equal(self):
        # the compounding of returns is where the extra read shows: market_orders is one of the runs it changes
        run = self._run('market_orders')

        self.assertEqual(len(run.frame), len(run.extra_frame))
        self.assertNotEqual(test_utils.frame_errors(run.frame, run.extra_frame), [])

    def test_the_observation_points_follow_the_last_write_of_the_signal(self):
        run = self._run('buy_and_hold')
        stream_events = list(run.broker.stream)

        points = [(i, s) for i, s in enumerate(stream_events) if isinstance(s.event, events.Observed)]
        self.assertGreater(len(points), 0)

        kinds = {s.event.clock_event for _, s in points}
        self.assertEqual(kinds, {SESSION_START, SESSION_END})

        for i, observed in points:
            previous = stream_events[i - 1].event

            if observed.event.clock_event == SESSION_END:
                # the prices are marked at the end of a session: the portfolio is revalued, then the point
                self.assertIsInstance(previous, events.Revalued)
            else:
                self.assertIsInstance(previous, (events.SessionStarted, events.Revalued, events.Split))

            self.assertEqual(observed.calendar_name, 'XNYS')

        # one point per session start
        self.assertEqual(sum(1 for _, s in points if s.event.clock_event == SESSION_START), len(run.frame))

    def test_a_failing_tracker_stops_the_run_at_its_tick(self):
        failing_session = 5
        calls = []
        original = test_utils.TestMetricsTracker.end_of_session

        def end_of_session(tracker, ts, calendar_name, ledger):
            calls.append(ts)

            if len(calls) == failing_session:
                raise RuntimeError('the tracker broke')

            return original(tracker, ts, calendar_name, ledger)

        # a strategy script is installed once per root directory: this run has its own name
        alias = 'market_orders_{}'.format(next(RUNS))
        _RecordingService.created.clear()

        with mock.patch.object(service, 'BrokerService', _RecordingService), \
                mock.patch.object(test_utils.TestMetricsTracker, 'end_of_session', end_of_session):
            with self.assertRaises(metrics_consumer.MetricsError) as raised:
                _run(self._pluto_tempdir, alias, self._environ, modules={alias: ALL_MODULES['market_orders']})

        # the error names the metrics consumer, the event of the stream and the failure
        message = str(raised.exception)
        self.assertIn('metrics consumer', message)
        self.assertIn('RuntimeError: the tracker broke', message)
        self.assertIsInstance(raised.exception.__cause__, RuntimeError)

        # it stopped at the tick of the failing read: five sessions started, five ended, nothing was published after
        # the observation point of the failing tick (the strategies did not run it, nothing was submitted)
        [broker] = _RecordingService.created
        stream_events = list(broker.stream)
        points = [s for s in stream_events if isinstance(s.event, events.Observed)]

        self.assertEqual(len(calls), failing_session)
        self.assertEqual(sum(1 for s in points if s.event.clock_event == SESSION_START), failing_session)
        self.assertEqual(sum(1 for s in points if s.event.clock_event == SESSION_END), failing_session)
        self.assertIs(stream_events[-1], points[-1])
        self.assertEqual(points[-1].event.dt, calls[-1])
        self.assertEqual(str(raised.exception).count('event {} '.format(points[-1].sequence)), 1)

        # the book is complete: its ledger is what the whole stream folds to
        replica = stream.Replica(broker.start_dt, broker.capital)
        stream.fold(stream_events, replica)
        self.assertEqual(
            replica.ledgers['XNYS'].get_state(calls[-1]), broker.get_ledger('XNYS').get_state(calls[-1]))


def _make_test(name):
    def test(self):
        self.check_computed_from_the_stream(name)
    test.__name__ = 'test_' + name
    return test


for _name in sorted(ALL_MODULES):
    setattr(StreamMetricsRunTests, 'test_' + _name, _make_test(_name))
del _name


class _RecordingTracker(object):
    def __init__(self, fail_at=None):
        self.reads = []
        self.fail_at = fail_at

    def _read(self, name, dt, calendar_name, ledger):
        if name == self.fail_at:
            raise RuntimeError('the tracker failed')
        self.reads.append((name, dt, calendar_name, ledger))

    def end_of_bar(self, dt, calendar_name, ledger):
        self._read('end_of_bar', dt, calendar_name, ledger)

    def start_of_session(self, dt, calendar_name, ledger):
        self._read('start_of_session', dt, calendar_name, ledger)

    def end_of_session(self, dt, calendar_name, ledger):
        self._read('end_of_session', dt, calendar_name, ledger)


class StreamMetricsUnitTests(unittest.TestCase):
    def _book(self, tracker):
        broker = service.BrokerService(None, START, CAPITAL, 'daily')
        sink = stream.InMemoryStream()
        broker.subscribe(sink)
        consumer = metrics_consumer.StreamMetrics(START, CAPITAL, tracker)
        broker.subscribe(consumer)
        broker.add_ledger('XNYS')
        return broker, sink, consumer

    def _signal(self, broker, clock_event, dt=DT):
        broker.sync_last_sale_prices('XNYS', (), dt)
        broker.revalue('XNYS')
        broker.observe('XNYS', clock_event, dt)

    def test_observe_publishes_a_marker_that_writes_nothing(self):
        broker, sink, _ = self._book(_RecordingTracker())
        self._signal(broker, SESSION_END)

        [last] = list(sink)[-1:]
        self.assertEqual(last.event, events.Observed(SESSION_END, DT))
        self.assertEqual(last.calendar_name, 'XNYS')
        self.assertEqual(last.sequence, len(sink))

        # the ledger of the book and its folded copy are in the same state
        replica = stream.Replica(START, CAPITAL)
        stream.fold(sink, replica)
        self.assertEqual(replica.ledgers['XNYS'].get_state(DT), broker.get_ledger('XNYS').get_state(DT))

    def test_the_consumer_reads_its_copy_at_each_point(self):
        tracker = _RecordingTracker()
        broker, _, consumer = self._book(tracker)

        self._signal(broker, SESSION_START, START)
        self._signal(broker, MINUTE_END, DT)
        self._signal(broker, SESSION_END, DT)

        self.assertIsNone(consumer.error)
        self.assertEqual(
            [(name, dt) for name, dt, _, _ in tracker.reads],
            [('start_of_session', START), ('end_of_bar', DT), ('end_of_session', DT)])

        for _, _, calendar_name, ledger in tracker.reads:
            self.assertEqual(calendar_name, 'XNYS')
            # the copy, not the ledger of the book
            self.assertIs(ledger, consumer.replica.ledgers['XNYS'])
            self.assertIsNot(ledger, broker.get_ledger('XNYS'))

    def test_a_failing_tracker_does_not_reach_the_write_of_the_book(self):
        tracker = _RecordingTracker(fail_at='end_of_session')
        broker, sink, consumer = self._book(tracker)

        self._signal(broker, SESSION_START, START)
        # does not raise
        self._signal(broker, SESSION_END, DT)

        self.assertIsInstance(consumer.error, RuntimeError)
        observed = [s for s in sink if isinstance(s.event, events.Observed)]
        self.assertEqual(consumer.error_sequence, observed[-1].sequence)

        # the book goes on: its writes complete and are published; the consumer ignores them
        reads = len(tracker.reads)
        count = len(sink)
        broker.sync_last_sale_prices('XNYS', (), DT)
        broker.revalue('XNYS')
        broker.observe('XNYS', SESSION_START, DT)

        self.assertGreater(len(sink), count)
        self.assertEqual(len(tracker.reads), reads)
        self.assertEqual(consumer.error_sequence, observed[-1].sequence)

    def test_an_unknown_clock_event_is_recorded_as_an_error(self):
        broker, _, consumer = self._book(_RecordingTracker())

        broker.observe('XNYS', 99, DT)

        self.assertIsInstance(consumer.error, ValueError)

    def test_a_gap_in_the_stream_is_recorded_as_an_error(self):
        consumer = metrics_consumer.StreamMetrics(START, CAPITAL, _RecordingTracker())

        consumer(events.StreamEvent('p', 2, 'XNYS', events.Revalued()))

        self.assertIsInstance(consumer.error, ValueError)
        self.assertEqual(consumer.error_sequence, 2)

    def test_an_event_delivered_twice_is_folded_and_read_once(self):
        tracker = _RecordingTracker()
        consumer = metrics_consumer.StreamMetrics(START, CAPITAL, tracker)
        observed = events.StreamEvent('p', 1, 'XNYS', events.Observed(SESSION_END, DT))

        consumer(observed)
        consumer(observed)

        self.assertIsNone(consumer.error)
        self.assertEqual([(name, dt) for name, dt, _, _ in tracker.reads], [('end_of_session', DT)])
        self.assertEqual(consumer.replica.sequence, 1)

        # the next event goes on from there
        consumer(events.StreamEvent('p', 2, 'XNYS', events.Observed(MINUTE_END, DT)))

        self.assertEqual([name for name, _, _, _ in tracker.reads], ['end_of_session', 'end_of_bar'])


class _FailedConsumer(metrics_consumer.StreamMetrics):
    """A consumer that failed at its last event."""

    def __init__(self, *args):
        super().__init__(*args)
        self.error = ValueError('the fold failed')
        self.error_sequence = 9


class HarnessTests(unittest.TestCase):
    def _run(self, consumer_class):
        broker = types.SimpleNamespace(start_dt=START, capital=CAPITAL, subscribe=lambda callback: None)
        manager = types.SimpleNamespace(broker=broker)
        # a loop without ticks: the controller is never driven, so nothing but the check after the loop can raise
        loop = types.SimpleNamespace(start=lambda process: iter(()))
        benchmark_source = types.SimpleNamespace(daily_returns_series=pd.Series([0.0, 0.0]))

        with mock.patch.object(metrics_consumer, 'StreamMetrics', consumer_class), \
                mock.patch.object(test_utils.controller, 'Controller', lambda *args: None):
            return test_utils.StrategyTester(None, None, 'strategy', manager).run(
                loop, benchmark_source, [START, DT], None, None, CAPITAL, None)

    def test_an_error_of_the_metrics_consumer_after_the_last_tick_is_raised(self):
        # the controller checks the consumer after the book phase of each tick: the last tick's submit phase is not covered
        with self.assertRaisesRegex(metrics_consumer.MetricsError, 'failed at event 9 .*ValueError: the fold failed'):
            self._run(_FailedConsumer)

    def test_a_run_without_error_returns_the_tracker(self):
        self.assertIsInstance(self._run(metrics_consumer.StreamMetrics), test_utils.TestMetricsTracker)


if __name__ == '__main__':
    unittest.main()
