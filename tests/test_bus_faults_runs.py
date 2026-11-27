"""Whole backtests over `SeededFaultBus`: what the phase protocol does when the bus misbehaves.

Short runs go over `SeededFaultBus(SerializingBus(InMemoryBus))`, one fault at a time, in the two modes of the wrapper: the default
(`networked` False: `run_until_idle` leaves nothing held, so a missing done is an immediate `PhaseError`) and `release_on_poll`
(`networked` True: held messages arrive only on `poll`, so `PhaseDriver` waits for them with its deadline, as for a participant in
another process). The runs: `buy_and_hold` (orders and fills, the first weeks of 2008) and the scenarios `dividends` (a cash
dividend paid to a position) and `cancel_orders` (orders, partial cancellation).

Two outcomes are allowed, never a third (a different result, or a run that does not end, is a bug of the protocol; a hang is cut
by the driver's deadline, patched to a second, and by an alarm):

* the frame of the run without faults, **exactly** (rtol 0, atol 0), or
* a `PhaseError`: the run aborted loudly.

The handlers are idempotent (`pluto/control/phases.py`: deterministic message ids, receivers that ignore a copy, publishers that retry
with the same id), so every fault of the bus that a participant that does answer can cause is **tolerated**: duplicate delivery
(`ack_lost`), a copy published later (`duplicate_publish`), a consumer that crashes and resumes from its checkpoint (`crash`), a
publish that fails (`publish_fail`) or fails after the bus took the message (`publish_ack_lost`), a delay and a lost delivery. The one
that stays an abort is a participant that never answers (`mute`): a `PhaseError` naming it, at once in memory, after the deadline on a
networked bus.
"""

import itertools
import signal
import unittest
from unittest import mock

import pandas as pd

from pluto import examples
from pluto.bus import InMemoryBus, SeededFaultBus, SerializingBus
from pluto.bus.faults import FaultConfig, FaultKind
from pluto.control import phases
from pluto.control.controller import process_manager
from pluto.test_utils import test_utils
from tests.pluto_scenarios import SCENARIO_MODULES
from tests.test_pluto_examples import _run
from tests.test_pluto_zipline_oracle import ZiplinePlutoFixture

RUNS = itertools.count()

# the runs: name -> (the module table, the end of the run or None for the whole scenario)
RUNS_OF = {
    'buy_and_hold': (examples.EXAMPLE_MODULES, pd.Timestamp('2008-01-25', tz='utc')),
    'dividends': (SCENARIO_MODULES, None),
    'cancel_orders': (SCENARIO_MODULES, None),
}

# one fault at a time, at rates that hit a run of a few hundred ticks many times; the kind that must show in the fault log
FAULTS = {
    'delay': (dict(delay=0.5, max_delay=20), FaultKind.DELAYED),
    'lose_delivery': (dict(lose_delivery=0.2), FaultKind.LOST_DELIVERY),
    'ack_lost': (dict(ack_lost=0.1), FaultKind.DUPLICATE_DELIVERY),
    'duplicate_publish': (dict(duplicate_publish=0.1), FaultKind.DUPLICATE_PUBLISH),
    'crash': (dict(crash=0.1), FaultKind.CRASH),
    'publish_fail': (dict(publish_fail=0.1), FaultKind.PUBLISH_FAILED),
    'publish_ack_lost': (dict(publish_ack_lost=0.1), FaultKind.PUBLISH_ACK_LOST),
}
MUTE = dict(mute=0.02)

SEEDS = (1, 2)
POLL_SEEDS = (1,)
POLL_RUNS = ('buy_and_hold', 'dividends')

DEADLINE = 1.0  # seconds a phase waits for a done on a networked bus
ALARM = 120  # seconds: a run that takes longer hangs


class Hang(Exception):
    pass


def _alarm(signum, frame):
    raise Hang('the run did not end within {} s'.format(ALARM))


class FaultRunTests(ZiplinePlutoFixture):
    def run_over(self, name, faults, seed):
        """(frame, None) of a run over the fault bus, or (None, the exception) when it aborted. The bus it used is in
        `self.bus`."""
        alias = '{}_faults_{}'.format(name, next(RUNS))
        modules, end = RUNS_OF[name]
        args, path = modules[name]
        args = args if end is None else dict(args, end=end)

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

    def assert_identical(self, baseline, frame, error, kind):
        self.assertIsNone(error, 'the run aborted: {!r}'.format(error))
        self.assertIn(kind, {fault.kind for fault in self.bus.fault_log()}, 'no fault was injected: the run proves nothing')
        self.assertEqual(test_utils.frame_errors(baseline, frame, rtol=0.0, atol=0.0), [])

    def test_the_faults_the_protocol_tolerates_give_the_identical_result(self):
        for name in RUNS_OF:
            baseline, error = self.run_over(name, FaultConfig(), 0)
            self.assertIsNone(error)

            for mode, release_on_poll, seeds in self.modes(name):
                for fault, (kwargs, kind) in FAULTS.items():
                    for seed in seeds:
                        with self.subTest(run=name, mode=mode, fault=fault, seed=seed):
                            frame, error = self.run_over(name, FaultConfig(release_on_poll=release_on_poll, **kwargs), seed)

                            self.assertIs(self.bus.networked, release_on_poll)
                            self.assert_identical(baseline, frame, error, kind)

    def test_a_participant_that_never_answers_is_a_clean_abort_never_a_wrong_result_or_a_hang(self):
        for name in RUNS_OF:
            for mode, release_on_poll, seeds in self.modes(name):
                for seed in seeds:
                    with self.subTest(run=name, mode=mode, seed=seed):
                        frame, error = self.run_over(name, FaultConfig(release_on_poll=release_on_poll, **MUTE), seed)

                        self.assertIn(FaultKind.MUTED, {fault.kind for fault in self.bus.fault_log()})
                        self.assertIsNone(frame)
                        self.assertIsInstance(error, phases.PhaseError, repr(error))
                        # who did not answer: a participant (no done) or the runtime whose batch the manager never took
                        self.assertRegex(str(error), 'no done from|no batch of orders from')

    @staticmethod
    def modes(name):
        """(mode, release_on_poll, seeds) to run for a run: the networked mode waits for its deadline when a participant is
        silent, so it runs for two of the three runs, with one seed; the third has one seed in the default mode."""
        modes = [('default', False, SEEDS if name != 'cancel_orders' else SEEDS[:1])]

        if name in POLL_RUNS:
            modes.append(('poll', True, POLL_SEEDS))

        return modes


if __name__ == '__main__':
    unittest.main()
