"""Scenarios beyond the five examples (tests/pluto_scenarios/): each strategy
runs through the fork's plain Zipline (run_algorithm) and through Pluto's
in-memory controller/controllable path in the same process and on the same test
bundle; the daily result frames must agree exactly (rtol=0, atol=0) on every
column both produce (tests/test_pluto_zipline_oracle.compare_frames), and
neither side may produce a column the other lacks.

Scenarios that still differ are listed in EXPECTED_FAILURES with the measured
difference and its cause; they run as unittest.expectedFailure so that they
stay visible and counted (an unexpected success fails the run).
"""
import unittest

from trading_calendars import register_calendar, get_calendar

from zipline.utils.run_algo import run_algorithm

from tests.pluto_scenarios import MODULES, SCENARIO_MODULES
from tests.test_pluto_examples import _run
from tests.test_pluto_zipline_oracle import ZiplinePlutoFixture, compare_frames

# name -> measured difference and cause (ALIGN_PROGRESS.md, step D)
EXPECTED_FAILURES = {}


def run_zipline(name, environ):
    mod = MODULES[name]
    args = mod._test_args()

    register_calendar('YAHOO', get_calendar('NYSE'), force=True)

    return run_algorithm(
        initialize=mod.initialize,
        handle_data=getattr(mod, 'handle_data', None),
        before_trading_start=getattr(mod, 'before_trading_start', None),
        bundle='test',
        environ=environ,
        start=args['start'],
        end=args['end'],
        capital_base=args['capital_base'],
        data_frequency=args['data_frequency'])


class PlutoScenarioTests(ZiplinePlutoFixture):
    def check_scenario(self, name):
        zipline_perf = run_zipline(name, self._environ)
        pluto_perf = _run(
            self._pluto_tempdir,
            name,
            None,
            self._environ,
            modules=SCENARIO_MODULES).frame()

        errors, zipline_only, pluto_only = compare_frames(zipline_perf, pluto_perf)

        print('\n[scenario] {}: {} rows, {} common columns compared; zipline only: {}; pluto only: {}'.format(
            name, len(zipline_perf),
            len(set(zipline_perf.columns) & set(pluto_perf.columns)),
            sorted(zipline_only), sorted(pluto_only)))

        self.assertEqual(errors, [], EXPECTED_FAILURES.get(name, ''))
        self.assertEqual(zipline_only, set())
        self.assertEqual(pluto_only, set())


def _make_test(name):
    def test(self):
        self.check_scenario(name)
    test.__name__ = 'test_' + name
    if name in EXPECTED_FAILURES:
        test.__doc__ = 'expected failure: ' + EXPECTED_FAILURES[name]
        test = unittest.expectedFailure(test)
    return test


for _name in sorted(SCENARIO_MODULES):
    setattr(PlutoScenarioTests, 'test_' + _name, _make_test(_name))
del _name
