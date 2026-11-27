"""Whole runs with the bus of the run on a NATS JetStream stream: every message of the run goes through the server and
back, and the result frame is the one of the in-memory run, every column exact (rtol=0, atol=0).

All the components are in this process (components in other processes are a later stage); what the run exercises is the
log-backed bus under the controller's lock-step: publish without waiting, the read before each dispatch, the wait for the
expected dones, the order of S3 over a mirror of the log. Skipped without a NATS server with JetStream
(``PLUTO_NATS_URL``, default nats://127.0.0.1:4222).
"""
import unittest
from unittest import mock

from parameterized import parameterized

from pluto import examples
from pluto.bus import InMemoryBus, SerializingBus
from pluto.bus.jetstream import DEFAULT_URL, JetStreamLog, server_available
from pluto.bus.log import LogBus
from pluto.control import phases
from pluto.control.controller import process_manager
from pluto.test_utils import test_utils

from tests.pluto_scenarios import SCENARIO_MODULES
from tests.test_pluto_examples import _run
from tests.test_pluto_phases_run import RUNS
from tests.test_pluto_zipline_oracle import ZiplinePlutoFixture


def jetstream_bus():
    return SerializingBus(LogBus(JetStreamLog(drop_on_close=True)), phases.codec())


@unittest.skipUnless(server_available(), 'no NATS server at ' + DEFAULT_URL)
class JetStreamRunTests(ZiplinePlutoFixture):
    def run_frame(self, name, modules, make_bus):
        alias = '{}_{}'.format(name, next(RUNS))
        built = []

        def make():
            built.append(make_bus())
            return built[-1]

        with mock.patch.object(process_manager, 'InMemoryBus', make):
            frame = _run(self._pluto_tempdir, alias, self._environ, modules={alias: modules[name]}).frame()

        self.assertEqual(len(built), 1)
        return frame, built[0]

    @parameterized.expand([
        ('buy_and_hold', examples.EXAMPLE_MODULES),
        ('cancel_orders', SCENARIO_MODULES),
        ('dividends', SCENARIO_MODULES),
    ])
    def test_a_run_over_jetstream_gives_the_in_memory_frame(self, name, modules):
        plain, _ = self.run_frame(name, modules, InMemoryBus)
        networked, bus = self.run_frame(name, modules, jetstream_bus)

        try:
            self.assertTrue(bus.networked)
            self.assertEqual(bus.dead_letters(), ())
        finally:
            bus.close()

        self.assertTrue((plain.index == networked.index).all())
        self.assertEqual(sorted(plain.columns), sorted(networked.columns))
        self.assertEqual(test_utils.frame_errors(plain, networked), [])


if __name__ == '__main__':
    unittest.main()
