"""Whole runs driven by phases on the bus: the same results over a serialising bus (every payload through bytes) as over
the in-memory one, the messages a run publishes, and the abort of a run when a participant fails."""
import contextlib
import itertools
import os
import tempfile
import unittest
from unittest import mock

from parameterized import parameterized

from pluto import examples
from pluto.bus import InMemoryBus, SerializingBus
from pluto.control import phases
from pluto.control.controller import process_manager
from pluto.test_utils import test_utils

from tests.pluto_scenarios import SCENARIO_MODULES, common
from tests.test_pluto_examples import _run
from tests.test_pluto_zipline_oracle import ZiplinePlutoFixture

ACCOUNT_STATES = 'strategy.account.*'


class RecordingBus(InMemoryBus):
    """An in-memory bus that keeps what was published."""

    def __init__(self):
        super().__init__()
        self.published = []

    def publish(self, topic, payload, **kwargs):
        self.published.append((topic, kwargs['type'], payload))
        return super().publish(topic, payload, **kwargs)

    def topics(self):
        """Messages published per topic; the broker states of the strategies (one topic each, named after the
        process id) count under ACCOUNT_STATES."""
        counts = {}
        for topic, _, _ in self.published:
            if topic.startswith('strategy.account.'):
                topic = ACCOUNT_STATES
            counts[topic] = counts.get(topic, 0) + 1
        return counts


@contextlib.contextmanager
def run_bus(serialising):
    """The bus the next run gets from its strategy manager (the default of a manager built without one): a recording
    in-memory bus, or the same behind a ``SerializingBus``. Yields the list of the recording buses built."""
    recorded = []

    def make():
        inner = RecordingBus()
        recorded.append(inner)
        return SerializingBus(inner, phases.codec()) if serialising else inner

    with mock.patch.object(process_manager, 'InMemoryBus', make):
        yield recorded


FAILING_STRATEGY = '''
from pluto.api import *

from tests.pluto_scenarios import common


def initialize(context):
    common.set_models(common.default_commission, common.default_slippage)


def handle_data(context, data):
    raise ValueError('the strategy broke')
'''

RUNS = itertools.count()


class PhasesRunTests(ZiplinePlutoFixture):
    def run_frame(self, name, modules, serialising):
        # a strategy script is installed once per root directory: each run of the same strategy has its own name
        alias = '{}_{}'.format(name, next(RUNS))

        with run_bus(serialising) as recorded:
            frame = _run(self._pluto_tempdir, alias, self._environ, modules={alias: modules[name]}).frame()

        self.assertEqual(len(recorded), 1)
        return frame, recorded[0]

    # the examples and two scenarios: orders, partial fills and cancellation; dividends (corporate actions at session start)
    @parameterized.expand([
        ('buy_and_hold', examples.EXAMPLE_MODULES),
        ('momentum_pipeline', examples.EXAMPLE_MODULES),
        ('cancel_orders', SCENARIO_MODULES),
        ('dividends', SCENARIO_MODULES),
    ])
    def test_serialising_bus_gives_identical_results(self, name, modules):
        plain, plain_bus = self.run_frame(name, modules, serialising=False)
        serialised, serialised_bus = self.run_frame(name, modules, serialising=True)

        self.assertTrue((plain.index == serialised.index).all())
        self.assertEqual(sorted(plain.columns), sorted(serialised.columns))
        self.assertEqual(test_utils.frame_errors(plain, serialised), [])

        # the same messages, and the inner bus of the serialising run carried nothing but bytes
        self.assertEqual(plain_bus.topics(), serialised_bus.topics())
        self.assertTrue(all(isinstance(payload, bytes) for _, _, payload in serialised_bus.published))
        self.assertEqual(serialised_bus.dead_letters(), ())

    def test_what_a_run_publishes(self):
        _, bus = self.run_frame('buy_and_hold', examples.EXAMPLE_MODULES, serialising=False)
        counts = bus.topics()
        ticks = counts[phases.TRADE.topic]

        # one account, trade and submit phase per tick, one batch of orders per tick, one book and one metrics phase
        # per signal (a signal per calendar and tick)
        self.assertGreater(ticks, 0)
        self.assertEqual(counts[phases.ACCOUNT.topic], ticks)
        self.assertEqual(counts[phases.SUBMIT.topic], ticks)
        self.assertEqual(counts[phases.ORDERS_TOPIC], ticks)
        # the watchlist goes with every clock update of a running strategy (not after it ended)
        self.assertGreater(counts[phases.WATCHLIST_TOPIC], 0)
        self.assertLessEqual(counts[phases.WATCHLIST_TOPIC], ticks)
        self.assertEqual(counts[phases.BOOK.topic], counts[phases.METRICS.topic])
        self.assertLessEqual(counts[phases.BOOK.topic], ticks)

        # the broker state of the strategy, once per tick, on its own topic
        self.assertEqual(counts[ACCOUNT_STATES], ticks)

        # every participant says done once per phase, on the done topic of the phase: book and metrics (1 each per
        # signal), account (broker and runtime), trade (runtime), submit (broker)
        self.assertEqual(counts[phases.BOOK.done_topic], counts[phases.BOOK.topic])
        self.assertEqual(counts[phases.METRICS.done_topic], counts[phases.METRICS.topic])
        self.assertEqual(counts[phases.ACCOUNT.done_topic], 2 * ticks)
        self.assertEqual(counts[phases.TRADE.done_topic], ticks)
        self.assertEqual(counts[phases.SUBMIT.done_topic], ticks)

        # the trade done of the strategy says how many orders it sent: the ones of its batches; the fills it was told
        # (in its broker states) are those orders
        published = [(topic, payload) for topic, _, payload in bus.published]
        batches = [p for t, p in published if t == phases.ORDERS_TOPIC]
        sent = [order.id for batch in batches for order in batch.orders]
        counted = sum(p.orders for t, p in published if t == phases.TRADE.done_topic)
        told = [c.transaction.order_id for t, p in published if t.startswith('strategy.account.')
                for c in p.transactions]

        self.assertGreater(len(sent), 0)
        self.assertEqual(counted, len(sent))
        self.assertEqual(
            sum(p.batches for t, p in published if t == phases.TRADE.done_topic), len(batches))
        self.assertEqual(sorted(told), sorted(sent))
        self.assertEqual(bus.dead_letters(), ())

    def test_a_failing_strategy_aborts_the_run_in_the_trade_phase(self):
        with tempfile.TemporaryDirectory() as path:
            source = os.path.join(path, 'failing_strategy.py')
            with open(source, 'w') as f:
                f.write(FAILING_STRATEGY)

            args = common.test_args(
                common.default_commission, common.default_slippage, '2014-01-02', '2014-01-31')

            with self.assertRaises(phases.PhaseError) as raised:
                _run(self._pluto_tempdir, 'failing_strategy', self._environ,
                     modules={'failing_strategy': (args, source)})

        message = str(raised.exception)
        self.assertIn('phase.trade', message)
        self.assertIn('ValueError: the strategy broke', message)


if __name__ == '__main__':
    unittest.main()
