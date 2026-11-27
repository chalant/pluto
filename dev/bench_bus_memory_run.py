"""Memory held by the bus of a whole `buy_and_hold` run, per structure (see `dev/bench_bus_memory.py`), in memory and over NATS.

    pytest dev/bench_bus_memory_run.py -s -q -W ignore

The bus is measured after the run, before it is closed. Needs a NATS server with JetStream for the second part.
"""
from unittest import mock

from pluto import examples
from pluto.bus import InMemoryBus, Retention, SerializingBus
from pluto.bus.jetstream import JetStreamLog
from pluto.bus.log import LogBus
from pluto.control import phases
from pluto.control.controller import process_manager

from dev import bench_bus_memory
from tests.test_pluto_examples import _run
from tests.test_pluto_phases_run import RUNS
from tests.test_pluto_zipline_oracle import ZiplinePlutoFixture


class RunMemory(ZiplinePlutoFixture):
    def measured(self, title, make_bus, **options):
        alias = 'buy_and_hold_{}'.format(next(RUNS))
        built = []

        def make():
            built.append(make_bus(**options))
            self.addCleanup(built[-1].close)
            return built[-1]

        with mock.patch.object(process_manager, 'InMemoryBus', make):
            _run(self._pluto_tempdir, alias, self._environ, modules={alias: examples.EXAMPLE_MODULES['buy_and_hold']}).frame()

        bus = built[0]
        inner = bus._inner
        messages = sum(inner._seq.values())  # published (the logs hold fewer under a window)
        bench_bus_memory.report(title, bus, messages)
        bus.close()

    def test_buy_and_hold(self):
        in_memory = lambda **o: SerializingBus(InMemoryBus(**o), phases.codec())
        over_nats = lambda **o: SerializingBus(LogBus(JetStreamLog(drop_on_close=True), **o), phases.codec())

        self.measured('buy_and_hold in memory', in_memory)
        self.measured('buy_and_hold in memory, window 1,000', in_memory, retention=Retention(max_messages=1000))
        self.measured('buy_and_hold over NATS', over_nats)
        self.measured('buy_and_hold over NATS, window 1,000', over_nats, retention=Retention(max_messages=1000))
