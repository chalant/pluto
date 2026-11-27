"""Whole backtests over `SeededFaultBus`: what the phase protocol does when the bus misbehaves.

Short runs go over `SeededFaultBus(SerializingBus(InMemoryBus))`, one fault at a time, in the two modes of the wrapper: the default
(`networked` False: `run_until_idle` leaves nothing held, so a missing done is an immediate `PhaseError`) and `release_on_poll`
(`networked` True: held messages arrive only on `poll`, so `PhaseDriver` waits for them with its deadline, as for a participant in
another process). The runs: `buy_and_hold` (orders and fills, the first weeks of 2008), the scenarios `dividends` (a cash dividend
paid to a position) and `cancel_orders` (orders, partial cancellation), and `buy_and_hold` in **minute** mode (a few days: the
minute clock goes back inside a session, so the clock times of a run are not ordered). A last class runs three fake runtimes on a
fake book, two calendars and two batches per trade event, over the same faults.

The handlers are idempotent (`pluto/control/phases.py`: deterministic message ids, receivers that ignore a copy, publishers that retry
with the same id), so every fault of the bus that a participant that does answer can cause is **tolerated**: the frame of the run is
the frame of the same run over a plain `InMemoryBus` **exactly** (rtol 0, atol 0): duplicate delivery (`ack_lost`), a copy published
later (`duplicate_publish`), a consumer that crashes and resumes from its checkpoint (`crash`), a publish that fails (`publish_fail`)
or fails after the bus took the message (`publish_ack_lost`), a delay, a lost delivery, and all of them at once. The one that is not
tolerated is a participant that never answers (`mute`): a `PhaseError` naming it, at once in memory, after the deadline on a
networked bus. A different result, or a run that does not end, is a bug of the protocol (a hang is cut by the driver's deadline,
patched to a second, and by an alarm).
"""

import itertools
import signal
import types
import unittest
from unittest import mock

import pandas as pd

from pluto import examples
from pluto.bus import InMemoryBus, SeededFaultBus, SerializingBus
from pluto.bus.faults import FaultConfig, FaultKind
from pluto.control import phases
from pluto.control.controller import controller, process_manager
from pluto.finance.metrics import consumer
from pluto.test_utils import test_utils
from tests.pluto_scenarios import SCENARIO_MODULES
from tests.test_pluto_examples import _run
from tests.test_pluto_phase_participants import FakeBehavior, FakeBroker, FakeProcess, TS, _calendar
from tests.test_pluto_zipline_oracle import ZiplinePlutoFixture

from protos import clock_pb2

RUNS = itertools.count()

# the runs: name -> (the module table, the module, what the run overrides in its arguments)
RUNS_OF = {
    'buy_and_hold': (examples.EXAMPLE_MODULES, 'buy_and_hold', dict(end=pd.Timestamp('2008-01-25', tz='utc'))),
    'dividends': (SCENARIO_MODULES, 'dividends', {}),
    'cancel_orders': (SCENARIO_MODULES, 'cancel_orders', {}),
    'buy_and_hold_minute': (
        examples.EXAMPLE_MODULES, 'buy_and_hold', dict(end=pd.Timestamp('2008-01-08', tz='utc'), data_frequency='minute')),
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
# all of them at once
COMBINED = dict(
    delay=0.2, max_delay=20, lose_delivery=0.05, ack_lost=0.05, duplicate_publish=0.05, crash=0.05, publish_fail=0.05,
    publish_ack_lost=0.05)
COMBINED_KINDS = {
    FaultKind.DELAYED, FaultKind.LOST_DELIVERY, FaultKind.DUPLICATE_DELIVERY, FaultKind.DUPLICATE_PUBLISH, FaultKind.CRASH,
    FaultKind.PUBLISH_FAILED, FaultKind.PUBLISH_ACK_LOST}
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
        `self.bus`. `faults` None: the plain `InMemoryBus` of the strategy manager."""
        alias = '{}_faults_{}'.format(name, next(RUNS))
        modules, module, overrides = RUNS_OF[name]
        args, path = modules[module]
        args = dict(args, **overrides)

        def make_bus():
            self.bus = SeededFaultBus(SerializingBus(InMemoryBus(), phases.codec()), seed, faults)
            return self.bus

        original = phases.PhaseDriver

        def driver(bus, producer_id=phases.CONTROLLER_ID, timeout=None):
            return original(bus, producer_id, timeout=DEADLINE)

        previous = signal.signal(signal.SIGALRM, _alarm)
        signal.alarm(ALARM)
        patches = [] if faults is None else [
            mock.patch.object(process_manager, 'InMemoryBus', make_bus), mock.patch.object(phases, 'PhaseDriver', driver)]

        try:
            for patch in patches:
                patch.start()

            return _run(self._pluto_tempdir, alias, self._environ, modules={alias: (args, path)}).frame(), None
        except Hang:
            raise
        except Exception as error:
            return None, error
        finally:
            for patch in patches:
                patch.stop()

            signal.alarm(0)
            signal.signal(signal.SIGALRM, previous)

    def baseline(self, name):
        frame, error = self.run_over(name, None, 0)
        self.assertIsNone(error)

        return frame

    def assert_identical(self, baseline, frame, error, kinds):
        self.assertIsNone(error, 'the run aborted: {!r}'.format(error))
        self.assertLessEqual(kinds, {fault.kind for fault in self.bus.fault_log()}, 'no fault was injected: the run proves nothing')
        self.assertEqual(test_utils.frame_errors(baseline, frame, rtol=0.0, atol=0.0), [])

    def test_a_run_over_the_wrapper_without_faults_is_the_plain_run(self):
        baseline = self.baseline('buy_and_hold_minute')

        frame, error = self.run_over('buy_and_hold_minute', FaultConfig(), 0)

        self.assertIsNone(error)
        self.assertEqual(test_utils.frame_errors(baseline, frame, rtol=0.0, atol=0.0), [])

    def test_the_faults_the_protocol_tolerates_give_the_identical_result(self):
        for name in RUNS_OF:
            baseline = self.baseline(name)

            for mode, release_on_poll, seeds in self.modes(name):
                for fault, (kwargs, kind) in FAULTS.items():
                    for seed in seeds:
                        with self.subTest(run=name, mode=mode, fault=fault, seed=seed):
                            frame, error = self.run_over(name, FaultConfig(release_on_poll=release_on_poll, **kwargs), seed)

                            self.assertIs(self.bus.networked, release_on_poll)
                            self.assert_identical(baseline, frame, error, {kind})

    def test_all_the_faults_at_once_give_the_identical_result(self):
        for name in ('buy_and_hold', 'buy_and_hold_minute'):
            baseline = self.baseline(name)

            for mode, release_on_poll, seeds in (('default', False, SEEDS), ('poll', True, POLL_SEEDS)):
                for seed in seeds:
                    with self.subTest(run=name, mode=mode, seed=seed):
                        frame, error = self.run_over(name, FaultConfig(release_on_poll=release_on_poll, **COMBINED), seed)

                        self.assert_identical(baseline, frame, error, COMBINED_KINDS)

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
        silent, so it runs for two of the runs, with one seed; the others have one or two seeds in the default mode."""
        modes = [('default', False, SEEDS if name in ('buy_and_hold', 'dividends') else SEEDS[:1])]

        if name in POLL_RUNS:
            modes.append(('poll', True, POLL_SEEDS))

        return modes


# the clock events of the fake run: a clock time carries two clock events, then the clock goes back (as the minute clock does)
TICKS = (
    (TS, clock_pb2.SESSION_START, ('XNYS',)),
    (TS, clock_pb2.BAR, ('XNYS',)),
    (TS + pd.Timedelta(minutes=1), clock_pb2.MINUTE_END, ('XNYS', 'XLON')),
    (TS - pd.Timedelta(minutes=13), clock_pb2.BAR, ('XNYS',)),
    (TS + pd.Timedelta(minutes=2), clock_pb2.SESSION_END, ('XNYS', 'XLON')),
)


class FakeBookRunTests(unittest.TestCase):
    """Three runtimes (the first sends two batches per trade event) on a fake book, over the same faults: the calls of the
    book, and what each runtime was told, are those of a plain bus. No example sends more than one batch per trade event
    (the controllable always sends one), nor runs two strategies: this is where both are exercised end to end."""

    def run_fake(self, bus):
        with mock.patch.object(process_manager.service, 'BrokerService', FakeBroker):
            setup = types.SimpleNamespace(start=TS, capital=1000.0, data_frequency='minute', leverage=1.0)
            manager = process_manager.StrategyManager(None, setup, bus=bus)
            ctl = controller.Controller(manager, None, FakeBehavior(), consumer.StreamMetrics(TS, 1000.0, None), phase_timeout=DEADLINE)
            processes = []

            for process_id in ('zzz', 'aaa', 'mmm'):
                process = FakeProcess(process_id)
                process.manager = manager
                process.batches = 2 if process_id == 'zzz' else 1
                manager.add_strategy(process)
                processes.append(process)

            for ts, evt, calendars in TICKS:
                ctl.clock_update(ts, evt, [(ts, evt, _calendar(name)) for name in calendars])

        return (
            manager.broker.calls,
            [(p.process_id, p.clock_events, len(p.account_updates)) for p in processes])

    def test_the_faults_the_protocol_tolerates_leave_the_run_as_it_is(self):
        expected = self.run_fake(InMemoryBus())
        self.assertEqual(sum(1 for call in expected[0] if call[0] == 'submit'), len(TICKS))

        for release_on_poll in (False, True):
            for fault, (kwargs, kind) in list(FAULTS.items()) + [('all', (COMBINED, None))]:
                for seed in (1, 2, 3):
                    with self.subTest(fault=fault, seed=seed, release_on_poll=release_on_poll):
                        bus = SeededFaultBus(
                            SerializingBus(InMemoryBus(), phases.codec()), seed, FaultConfig(release_on_poll=release_on_poll, **kwargs))

                        self.assertEqual(self.run_fake(bus), expected)
                        self.assertTrue(bus.fault_log())


if __name__ == '__main__':
    unittest.main()
