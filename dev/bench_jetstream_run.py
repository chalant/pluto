"""Time of a whole `buy_and_hold` run on the in-memory bus and over NATS JetStream, with `publish` waiting for the
broker's acknowledgement (what the bus did before `Receipt.position` became lazy) and without waiting (the bus as it is).

    pytest dev/bench_jetstream_run.py -s -q -W ignore

needs a NATS server with JetStream (`PLUTO_NATS_URL`). The modes alternate twice (the machine is shared: compare the pairs).
"""
import time
from unittest import mock

from pluto import examples
from pluto.bus import InMemoryBus, SerializingBus
from pluto.bus.jetstream import JetStreamLog
from pluto.bus.log import LogBus
from pluto.control import phases
from pluto.control.controller import process_manager

from tests.test_pluto_examples import _run
from tests.test_pluto_phases_run import RUNS
from tests.test_pluto_zipline_oracle import ZiplinePlutoFixture


class WaitingLog(JetStreamLog):
    """`append` waits until the broker has stored the message (it reads it back at once): a synchronous publish."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._held = []

    def append(self, topic, data):
        super().append(topic, data)
        self._held.extend(super().read_new(0, False))

    def read_new(self, wait=0.0, complete=True):
        held, self._held = self._held, []
        return held + super().read_new(wait, complete)


def networked(log_type):
    def make():
        return SerializingBus(LogBus(log_type(drop_on_close=True)), phases.codec())

    return make


class RunBench(ZiplinePlutoFixture):
    def timed(self, make_bus):
        alias = 'buy_and_hold_{}'.format(next(RUNS))
        built = []

        def make():
            built.append(make_bus())
            self.addCleanup(built[-1].close)  # a failed run leaves no stream (close is idempotent)
            return built[-1]

        started = time.perf_counter()

        with mock.patch.object(process_manager, 'InMemoryBus', make):
            _run(self._pluto_tempdir, alias, self._environ, modules={alias: examples.EXAMPLE_MODULES['buy_and_hold']}).frame()

        seconds = time.perf_counter() - started
        messages = sum(len(log) for log in built[0]._inner._logs.values()) if hasattr(built[0], '_inner') else None

        for bus in built:
            bus.close()

        return seconds, messages

    def test_buy_and_hold(self):
        modes = (
            ('in memory', InMemoryBus),
            ('JetStream, publish waits for the ack', networked(WaitingLog)),
            ('JetStream, publish does not wait', networked(JetStreamLog)))

        for round_ in range(2):
            for name, make in modes:
                seconds, messages = self.timed(make)
                print('\nBENCH round {} {:<40} {:7.1f} s{}'.format(
                    round_ + 1, name, seconds, '' if messages is None else '  ({} messages)'.format(messages)))
