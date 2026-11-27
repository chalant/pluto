"""Whole runs with the bus of the run on a NATS JetStream stream: every message of the run goes through the server and
back, and the result frame is the one of the in-memory run, every column exact (rtol=0, atol=0).

All the components are in this process (components in other processes are a later stage); what the run exercises is the
log-backed bus under the controller's lock-step: publish without waiting, the read before each dispatch, the wait for the
expected dones, the order of S3 over a mirror of the log. Skipped without a NATS server with JetStream
(``PLUTO_NATS_URL``, default nats://127.0.0.1:4222; failures with ``PLUTO_REQUIRE_NATS=1``).
"""
import unittest
from unittest import mock

from parameterized import parameterized

from tests import nats_support

nats_support.require_module()

from pluto import examples  # noqa: E402
from pluto.bus import InMemoryBus, SerializingBus  # noqa: E402
from pluto.bus.jetstream import JetStreamLog  # noqa: E402
from pluto.bus.log import LogBus  # noqa: E402
from pluto.control import phases  # noqa: E402
from pluto.control.controller import process_manager  # noqa: E402
from pluto.test_utils import test_utils  # noqa: E402

from tests.pluto_scenarios import SCENARIO_MODULES  # noqa: E402
from tests.test_pluto_examples import _run  # noqa: E402
from tests.test_pluto_phases_run import RUNS  # noqa: E402
from tests.test_pluto_zipline_oracle import ZiplinePlutoFixture  # noqa: E402


def jetstream_bus():
    return SerializingBus(LogBus(JetStreamLog(drop_on_close=True)), phases.codec())


@nats_support.needs_server
class JetStreamRunTests(ZiplinePlutoFixture):
    def run_frame(self, name, modules, make_bus):
        alias = '{}_{}'.format(name, next(RUNS))
        built = []

        def make():
            bus = make_bus()
            built.append(bus)
            # a run that fails leaves no stream on the shared server (close is idempotent: the test closes it too)
            self.addCleanup(bus.close)
            return bus

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

        self.assertTrue(bus.networked)
        self.assertEqual(bus.dead_letters(), ())
        bus.close()  # raises what the broker refused, if anything

        self.assertTrue((plain.index == networked.index).all())
        self.assertEqual(sorted(plain.columns), sorted(networked.columns))
        self.assertEqual(test_utils.frame_errors(plain, networked), [])


if __name__ == '__main__':
    unittest.main()
