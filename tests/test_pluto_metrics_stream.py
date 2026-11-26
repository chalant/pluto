"""Metrics computed from the event stream of the book equal the metrics of the controller's path.

``pluto.finance.metrics.consumer.StreamMetrics`` subscribes to the broker service, folds the stream into its own copy of
the book and reads it at the observation points (``events.Observed``); the tracker it calls is a second harness tracker
(``test_utils.TestMetricsTracker``) of the run. For each of the 5 examples and 21 scenarios its frame must be the one of
the tracker the metrics phase calls with the ledger of the book: same length, index and columns, every column exact
(rtol=0, atol=0), lists included. A consumer that reads once more than the book revalued must not be equal.
"""
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


class _RecordingService(service.BrokerService):
    """The broker service of a run, with its stream kept and its start and capital known."""

    created = []

    def __init__(self, asset_finder, start_dt, capital, data_frequency):
        super().__init__(asset_finder, start_dt, capital, data_frequency)
        self.start_dt = start_dt
        self.capital = capital
        self.stream = stream.InMemoryStream()
        self.subscribe(self.stream)

        self.created.append(self)


class _Consumed(object):
    """A consumer of a run and the tracker it called."""

    def __init__(self, consumer, tracker):
        self.consumer = consumer
        self.tracker = tracker

    @property
    def frame(self):
        return self.tracker.frame()


class _Run(object):
    """What a run of an example or scenario gave: the frame of the metrics phase's tracker, the consumers (by name)
    with their trackers, the stream."""

    def __init__(self, primary, consumed, broker):
        self.primary = primary
        self.consumed = consumed
        self.broker = broker


class _ExtraRevalue(metrics_consumer.StreamMetrics):
    """Reads (revalues) its copy of the book once more than the book did: after each mark of prices."""

    def __call__(self, stream_event):
        super().__call__(stream_event)

        if self.error is None and isinstance(stream_event.event, events.PricesMarked):
            self.replica.ledgers[stream_event.calendar_name].revalue()


CONSUMERS = {'stream': metrics_consumer.StreamMetrics, 'extra_revalue': _ExtraRevalue}


def run_with_consumers(pluto_tempdir, name, environ):
    """Runs ``name`` through the controller's path with the ``CONSUMERS`` subscribed to the book's stream, next to the
    tracker of the metrics phase; the tracker of each consumer is built like the harness one, with the same
    arguments."""
    _RecordingService.created.clear()
    consumed = {}
    original_init = test_utils.TestMetricsTracker.__init__

    def init(self, *args, **kwargs):
        original_init(self, *args, **kwargs)

        [broker] = _RecordingService.created

        for key, consumer_class in CONSUMERS.items():
            twin = test_utils.TestMetricsTracker.__new__(test_utils.TestMetricsTracker)
            original_init(twin, *args, **kwargs)

            consumer = consumer_class(broker.start_dt, broker.capital, twin)
            broker.subscribe(consumer)
            consumed[key] = _Consumed(consumer, twin)

    with mock.patch.object(service, 'BrokerService', _RecordingService), \
            mock.patch.object(test_utils.TestMetricsTracker, '__init__', init):
        tracker = _run(pluto_tempdir, name, environ, modules=ALL_MODULES)

    [broker] = _RecordingService.created
    return _Run(tracker.frame(), consumed, broker)


class StreamMetricsRunTests(ZiplinePlutoFixture):
    _runs = {}

    def _run(self, name):
        runs = type(self)._runs

        if name not in runs:
            runs[name] = run_with_consumers(self._pluto_tempdir, name, self._environ)

        return runs[name]

    def check_equal(self, name):
        run = self._run(name)
        consumed = run.consumed['stream']

        self.assertIsNone(consumed.consumer.error)

        primary, frame = run.primary, consumed.frame
        self.assertGreater(len(primary), 0)
        self.assertEqual(len(primary), len(frame))
        self.assertTrue((primary.index == frame.index).all())
        self.assertEqual(set(primary.columns), set(frame.columns))
        self.assertEqual(test_utils.frame_errors(primary, frame, rtol=0.0, atol=0.0), [])

    def test_a_read_more_than_the_book_revalued_is_not_equal(self):
        # the compounding of returns is where the extra read shows: market_orders is one of the runs it changes
        run = self._run('market_orders')
        consumed = run.consumed['extra_revalue']

        self.assertIsNone(consumed.consumer.error)
        self.assertEqual(len(run.primary), len(consumed.frame))
        self.assertNotEqual(test_utils.frame_errors(run.primary, consumed.frame), [])

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

        # two points per session, and the sequence of the stream has no gap with them in it
        self.assertEqual([s.sequence for s in stream_events], list(range(1, len(stream_events) + 1)))
        self.assertEqual(sum(1 for _, s in points if s.event.clock_event == SESSION_START), len(run.primary))


def _make_test(name):
    def test(self):
        self.check_equal(name)
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


if __name__ == '__main__':
    unittest.main()
