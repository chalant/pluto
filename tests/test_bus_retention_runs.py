"""Whole runs with a retention window on the bus of the run: the result is the unlimited run's, exact, and the logs stay small.

A run never replays: the controller and the participants only need what is in flight, so a window larger than one tick's messages
changes nothing but the memory held. The window here is far smaller than a run (a `buy_and_hold` run publishes about 76,000
messages), so the logs are trimmed thousands of times during it.
"""
import unittest
from unittest import mock

from parameterized import parameterized

from pluto import examples
from pluto.bus import InMemoryBus, Retention, SerializingBus
from pluto.bus.log import LogBus
from pluto.control import phases
from pluto.control.controller import process_manager
from pluto.test_utils import test_utils

from tests import nats_support
from tests.pluto_scenarios import SCENARIO_MODULES
from tests.test_pluto_examples import _run
from tests.test_pluto_phases_run import RUNS
from tests.test_pluto_zipline_oracle import ZiplinePlutoFixture

WINDOW = 200


def windowed_memory_bus():
    return SerializingBus(InMemoryBus(retention=Retention(max_messages=WINDOW)), phases.codec())


def windowed_jetstream_bus():
    return SerializingBus(
        LogBus(nats_support.jetstream.JetStreamLog(drop_on_close=True), retention=Retention(max_messages=WINDOW)),
        phases.codec())


class RetentionRunTests(ZiplinePlutoFixture):
    def run_frame(self, name, modules, make_bus):
        alias = '{}_{}'.format(name, next(RUNS))
        built = []

        def make():
            bus = make_bus()
            built.append(bus)
            self.addCleanup(bus.close)
            return bus

        with mock.patch.object(process_manager, 'InMemoryBus', make):
            frame = _run(self._pluto_tempdir, alias, self._environ, modules={alias: modules[name]}).frame()

        self.assertEqual(len(built), 1)
        return frame, built[0]

    def check(self, name, modules, make_bus):
        plain, _ = self.run_frame(name, modules, InMemoryBus)
        windowed, bus = self.run_frame(name, modules, make_bus)
        inner = bus._inner
        published = sum(inner._seq.values())

        # the logs were trimmed many times and hold a window each, whatever the length of the run
        self.assertGreater(published, 20 * WINDOW)
        self.assertTrue(inner._logs)
        self.assertTrue(all(len(log) <= WINDOW + WINDOW // 4 for log in inner._logs.values()))
        self.assertTrue(any(base > 0 for base in inner._base.values()))
        self.assertEqual(bus.dead_letters(), ())
        bus.close()

        self.assertTrue((plain.index == windowed.index).all())
        self.assertEqual(sorted(plain.columns), sorted(windowed.columns))
        self.assertEqual(test_utils.frame_errors(plain, windowed), [])

    @parameterized.expand([
        ('buy_and_hold', examples.EXAMPLE_MODULES),
        ('dividends', SCENARIO_MODULES),
    ])
    def test_a_windowed_in_memory_run_gives_the_unlimited_frame(self, name, modules):
        self.check(name, modules, windowed_memory_bus)

    @nats_support.needs_server
    def test_a_windowed_run_over_jetstream_gives_the_unlimited_frame(self):
        self.check('dividends', SCENARIO_MODULES, windowed_jetstream_bus)


if __name__ == '__main__':
    unittest.main()
