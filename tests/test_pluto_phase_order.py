"""Whole runs on buses that deliver in other orders than S3 (``(logical_ts, phase, producer_id, seq)``), to find what a run
relies on besides the lock-step of the phases.

On a networked bus the order in which two processes' messages of one phase reach a reader is not the order of their
producer ids: a run must not depend on it. ``ShuffledBus`` gives the messages of one ``(logical_ts, phase)`` a random
order (the same seed gives the same order; per-lane FIFO still holds: a producer's messages of one key stay in order),
``ArrivalBus`` delivers strictly in the order of publication (what a reader that dispatches as the log arrives does).
Both are legal runs of a networked bus; every run must give the frame of the in-memory run exactly.
"""
import itertools
import random
import unittest
from heapq import heappush
from unittest import mock

from parameterized import parameterized

from pluto import examples
from pluto.bus import InMemoryBus
from pluto.control.controller import process_manager
from pluto.test_utils import test_utils

from tests.pluto_scenarios import SCENARIO_MODULES
from tests.test_pluto_examples import _run
from tests.test_pluto_phases_run import RUNS
from tests.test_pluto_zipline_oracle import ZiplinePlutoFixture
from tests import test_pluto_phase_participants as participants_tests


class _OtherOrderBus(InMemoryBus):
    """An in-memory bus whose dispatch key replaces ``(logical_ts, producer_id, seq)`` by ``order(envelope)``."""

    def _order(self, envelope, offset):
        raise NotImplementedError

    def _push(self, lane):
        offset, envelope = lane.queue[0]
        owner = lane.owner

        heappush(
            self._heap,
            (
                self._order(envelope, offset),
                owner.rank,
                owner.id,
                envelope.topic,
                envelope.key,
                lane.stamp,
                lane,
            ),
        )


class ShuffledBus(_OtherOrderBus):
    """The messages of one ``(logical_ts, phase)`` in a seeded random order."""

    def __init__(self, seed=1):
        super().__init__()
        self._random = random.Random(seed)
        self._draws = {}

    def _order(self, envelope, offset):
        draw = self._draws.get((envelope.topic, offset))

        if draw is None:
            draw = self._draws[(envelope.topic, offset)] = self._random.random()

        return envelope.logical_ts, envelope.phase, draw


class ArrivalBus(_OtherOrderBus):
    """The messages in the order they were published, whatever their time and phase."""

    def __init__(self):
        super().__init__()
        self._arrivals = itertools.count()
        self._numbers = {}

    def _order(self, envelope, offset):
        number = self._numbers.get((envelope.topic, offset))

        if number is None:
            number = self._numbers[(envelope.topic, offset)] = next(self._arrivals)

        return (number,)


class RunOrderTests(ZiplinePlutoFixture):
    def frame(self, name, modules, make_bus):
        alias = '{}_{}'.format(name, next(RUNS))

        with mock.patch.object(process_manager, 'InMemoryBus', make_bus):
            return _run(self._pluto_tempdir, alias, self._environ, modules={alias: modules[name]}).frame()

    @parameterized.expand([
        ('buy_and_hold', examples.EXAMPLE_MODULES),
        ('cancel_orders', SCENARIO_MODULES),
        ('dividends', SCENARIO_MODULES),
    ])
    def test_the_frame_does_not_depend_on_the_delivery_order_of_a_phase(self, name, modules):
        expected = self.frame(name, modules, InMemoryBus)

        for make_bus in (ShuffledBus, ArrivalBus):
            frame = self.frame(name, modules, make_bus)

            self.assertTrue((expected.index == frame.index).all())
            self.assertEqual(sorted(expected.columns), sorted(frame.columns))
            self.assertEqual(test_utils.frame_errors(expected, frame), [])


class FaultyShuffledBus(participants_tests.FaultyBus, ShuffledBus):
    pass


class FaultyArrivalBus(participants_tests.FaultyBus, ArrivalBus):
    pass


class ShuffledRunTests(participants_tests.RunTests):
    """The tests of the strategy manager and the controller with a fake book and three runtimes, on a bus that gives
    the messages of a phase a random order."""
    make_bus = FaultyShuffledBus


class ArrivalRunTests(participants_tests.RunTests):
    """The same, on a bus that delivers in the order of publication."""
    make_bus = FaultyArrivalBus


if __name__ == '__main__':
    unittest.main()
