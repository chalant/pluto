"""Whole backtests over `SeededFaultBus`: what the phase protocol does when the bus misbehaves.

A short `buy_and_hold` run (the first weeks of 2008) goes over `SeededFaultBus(SerializingBus(InMemoryBus))`, one fault at a
time, in the two modes of the wrapper: the default (`networked` False: `run_until_idle` leaves nothing held, so a missing done is
an immediate `PhaseError`) and `release_on_poll` (`networked` True: held messages arrive only on `poll`, so `PhaseDriver` waits
for them with its deadline, as for a participant in another process).

The outcome of a run must be one of two things, never a third:

* the frame of the run without faults, exactly (the protocol tolerated the fault), or
* a `PhaseError` (or a `PublishFailed` that a publisher did not retry): the run aborted loudly.

A different result, or a run that does not end, is a bug of the protocol (a hang is cut by the deadline of the driver, patched to
a second here, and by an alarm). Today the handlers are not idempotent, so duplicates, restarts, silent participants and failed
publishes abort the run; delays and lost deliveries are tolerated. When the handlers become idempotent the aborts become
identical frames and this test still passes; `TOLERATED` lists what must already be identical and tightens with them.
"""

import itertools
import signal
import unittest
from unittest import mock

import pandas as pd

from pluto import examples
from pluto.bus import InMemoryBus, PublishFailed, SeededFaultBus, SerializingBus
from pluto.bus.faults import FaultConfig
from pluto.control import phases
from pluto.control.controller import process_manager
from pluto.test_utils import test_utils
from tests.test_pluto_examples import _run
from tests.test_pluto_zipline_oracle import ZiplinePlutoFixture

RUNS = itertools.count()

# one fault at a time, at rates that hit a run of a few hundred ticks many times
FAULTS = {
    "delay": dict(delay=0.5, max_delay=20),
    "lose_delivery": dict(lose_delivery=0.2),
    "ack_lost": dict(ack_lost=0.1),
    "duplicate_publish": dict(duplicate_publish=0.1),
    "crash": dict(crash=0.1),
    "mute": dict(mute=0.02),
    "publish_ack_lost": dict(publish_ack_lost=0.1),
    "publish_fail": dict(publish_fail=0.1),
}

# the faults the protocol tolerates today: the frame must equal the run without faults
TOLERATED = {"delay", "lose_delivery"}

DEADLINE = 1.0  # seconds a phase waits for a done on a networked bus
ALARM = 120  # seconds: a run that takes longer hangs


class Hang(Exception):
    pass


def _alarm(signum, frame):
    raise Hang("the run did not end within {} s".format(ALARM))


class FaultRunTests(ZiplinePlutoFixture):
    def run_over(self, faults, seed):
        """(frame, None) of a run over the fault bus, or (None, the exception) when it aborted. The bus it used is in
        `self.bus`."""
        alias = 'buy_and_hold_faults_{}'.format(next(RUNS))
        args, path = examples.EXAMPLE_MODULES['buy_and_hold']
        args = dict(args, end=pd.Timestamp('2008-02-15', tz='utc'))

        def make_bus():
            self.bus = SeededFaultBus(SerializingBus(InMemoryBus(), phases.codec()), seed, faults)
            return self.bus

        original = phases.PhaseDriver

        def driver(bus, producer_id=phases.CONTROLLER_ID, timeout=None):
            return original(bus, producer_id, timeout=DEADLINE)

        previous = signal.signal(signal.SIGALRM, _alarm)
        signal.alarm(ALARM)

        try:
            with mock.patch.object(process_manager, 'InMemoryBus', make_bus), \
                    mock.patch.object(phases, 'PhaseDriver', driver):
                return _run(self._pluto_tempdir, alias, self._environ, modules={alias: (args, path)}).frame(), None
        except Hang:
            raise
        except Exception as error:
            return None, error
        finally:
            signal.alarm(0)
            signal.signal(signal.SIGALRM, previous)

    def test_a_run_is_identical_or_aborts_never_wrong_and_never_hangs(self):
        baseline, error = self.run_over(FaultConfig(), 0)
        self.assertIsNone(error)

        for mode, release_on_poll in (('default', False), ('poll', True)):
            for name, kwargs in FAULTS.items():
                with self.subTest(mode=mode, fault=name):
                    frame, error = self.run_over(FaultConfig(release_on_poll=release_on_poll, **kwargs), 1)

                    self.assertTrue(self.bus.fault_log(), 'no fault was injected: the run proves nothing')
                    self.assertIs(self.bus.networked, release_on_poll)

                    if frame is not None:
                        # tolerated: every number of the frame is the baseline's
                        self.assertEqual(test_utils.frame_errors(baseline, frame), [])
                    else:
                        self.assertNotIn(name, TOLERATED, 'the protocol tolerates {}: {!r}'.format(name, error))
                        self.assertIsInstance(error, (phases.PhaseError, PublishFailed), repr(error))


if __name__ == '__main__':
    unittest.main()
