#
# Copyright 2015 Quantopian, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Tests for the zipline.assets package
"""
from collections import namedtuple
from datetime import timedelta
from functools import partial
import os
import pickle
import string
import sys
from types import GetSetDescriptorType
from unittest import TestCase
import uuid
import warnings

from nose_parameterized import parameterized
import numpy as np
from numpy import full, int32, int64
import pandas as pd
from six import viewkeys
import sqlalchemy as sa

from zipline.assets import (
    Asset,
    ExchangeInfo,
    Equity,
    Future,
    AssetDBWriter,
    AssetFinder,
)
from zipline.assets.assets import OwnershipPeriod
from zipline.assets.synthetic import (
    make_commodity_future_info,
    make_rotating_equity_info,
    make_simple_equity_info,
)
from six import itervalues, integer_types
from toolz import valmap, concat

from zipline.assets.asset_writer import (
    check_version_info,
    write_version_info,
    _futures_defaults,
    SQLITE_MAX_VARIABLE_NUMBER,
)
from zipline.assets.asset_db_schema import ASSET_DB_VERSION
from zipline.assets.asset_db_migrations import (
    downgrade
)
from zipline.errors import (
    EquitiesNotFound,
    FutureContractsNotFound,
    MultipleSymbolsFound,
    MultipleSymbolsFoundForFuzzySymbol,
    MultipleValuesFoundForField,
    MultipleValuesFoundForSid,
    NoValueForSid,
    AssetDBVersionError,
    SameSymbolUsedAcrossCountries,
    SidsNotFound,
    SymbolNotFound,
    AssetDBImpossibleDowngrade,
    ValueNotFoundForField,
)
from zipline.testing import (
    all_subindices,
    empty_assets_db,
    parameter_space,
    powerset,
    tmp_assets_db,
    tmp_asset_finder,
)
from zipline.testing.predicates import assert_equal, assert_not_equal
from zipline.testing.fixtures import (
    WithAssetFinder,
    ZiplineTestCase,
    WithTradingCalendars,
    WithTmpDir,
    WithInstanceTmpDir,
)
from zipline.utils.range import range


Case = namedtuple('Case', 'finder inputs as_of country_code expected')

minute = pd.Timedelta(minutes=1)


class AssetFinderTestCase(WithTradingCalendars, ZiplineTestCase):
    asset_finder_type = AssetFinder

    def write_assets(self, **kwargs):
        self._asset_writer.write(**kwargs)

    def init_instance_fixtures(self):
        super(AssetFinderTestCase, self).init_instance_fixtures()

        conn = self.enter_instance_context(empty_assets_db())
        self._asset_writer = AssetDBWriter(conn)
        self.asset_finder = self.asset_finder_type(conn)

    def test_blocked_lookup_symbol_query(self):
        # we will try to query for more variables than sqlite supports
        # to make sure we are properly chunking on the client side
        as_of = pd.Timestamp('2013-01-01', tz='UTC')
        # we need more sids than we can query from sqlite
        nsids = SQLITE_MAX_VARIABLE_NUMBER + 10
        sids = range(nsids)
        frame = pd.DataFrame.from_records(
            [
                {
                    'sid': sid,
                    'symbol':  'TEST.%d' % sid,
                    'start_date': as_of.value,
                    'end_date': as_of.value,
                    'exchange': uuid.uuid4().hex
                }
                for sid in sids
            ]
        )
        self.write_assets(equities=frame)
        assets = self.asset_finder.retrieve_equities(sids)
        assert_equal(viewkeys(assets), set(sids))

    def test_lookup_symbol_delimited(self):
        as_of = pd.Timestamp('2013-01-01', tz='UTC')
        frame = pd.DataFrame.from_records(
            [
                {
                    'sid': i,
                    'symbol':  'TEST.%d' % i,
                    'company_name': "company%d" % i,
                    'start_date': as_of.value,
                    'end_date': as_of.value,
                    'exchange': uuid.uuid4().hex
                }
                for i in range(3)
            ]
        )
        self.write_assets(equities=frame)
        finder = self.asset_finder
        asset_0, asset_1, asset_2 = (
            finder.retrieve_asset(i) for i in range(3)
        )

        # we do it twice to catch caching bugs
        for i in range(2):
            with self.assertRaises(SymbolNotFound):
                finder.lookup_symbol('TEST', as_of)
            with self.assertRaises(SymbolNotFound):
                finder.lookup_symbol('TEST1', as_of)
            # '@' is not a supported delimiter
            with self.assertRaises(SymbolNotFound):
                finder.lookup_symbol('TEST@1', as_of)

            # Adding an unnecessary fuzzy shouldn't matter.
            for fuzzy_char in ['-', '/', '_', '.']:
                self.assertEqual(
                    asset_1,
                    finder.lookup_symbol('TEST%s1' % fuzzy_char, as_of)
                )

    def test_lookup_symbol_fuzzy(self):
        metadata = pd.DataFrame.from_records([
            {'symbol': 'PRTY_HRD', 'exchange': "TEST"},
            {'symbol': 'BRKA', 'exchange': "TEST"},
            {'symbol': 'BRK_A', 'exchange': "TEST"},
        ])
        self.write_assets(equities=metadata)
        finder = self.asset_finder
        dt = pd.Timestamp('2013-01-01', tz='UTC')

        # Try combos of looking up PRTYHRD with and without a time or fuzzy
        # Both non-fuzzys get no result
        with self.assertRaises(SymbolNotFound):
            finder.lookup_symbol('PRTYHRD', None)
        with self.assertRaises(SymbolNotFound):
            finder.lookup_symbol('PRTYHRD', dt)
        # Both fuzzys work
        self.assertEqual(0, finder.lookup_symbol('PRTYHRD', None, fuzzy=True))
        self.assertEqual(0, finder.lookup_symbol('PRTYHRD', dt, fuzzy=True))

        # Try combos of looking up PRTY_HRD, all returning sid 0
        self.assertEqual(0, finder.lookup_symbol('PRTY_HRD', None))
        self.assertEqual(0, finder.lookup_symbol('PRTY_HRD', dt))
        self.assertEqual(0, finder.lookup_symbol('PRTY_HRD', None, fuzzy=True))
        self.assertEqual(0, finder.lookup_symbol('PRTY_HRD', dt, fuzzy=True))

        # Try combos of looking up BRKA, all returning sid 1
        self.assertEqual(1, finder.lookup_symbol('BRKA', None))
        self.assertEqual(1, finder.lookup_symbol('BRKA', dt))
        self.assertEqual(1, finder.lookup_symbol('BRKA', None, fuzzy=True))
        self.assertEqual(1, finder.lookup_symbol('BRKA', dt, fuzzy=True))

        # Try combos of looking up BRK_A, all returning sid 2
        self.assertEqual(2, finder.lookup_symbol('BRK_A', None))
        self.assertEqual(2, finder.lookup_symbol('BRK_A', dt))
        self.assertEqual(2, finder.lookup_symbol('BRK_A', None, fuzzy=True))
        self.assertEqual(2, finder.lookup_symbol('BRK_A', dt, fuzzy=True))

    def test_lookup_symbol_change_ticker(self):
        T = partial(pd.Timestamp, tz='utc')
        metadata = pd.DataFrame.from_records(
            [
                # sid 0
                {
                    'symbol': 'A',
                    'asset_name': 'Asset A',
                    'start_date': T('2014-01-01'),
                    'end_date': T('2014-01-05'),
                    'exchange': "TEST",
                },
                {
                    'symbol': 'B',
                    'asset_name': 'Asset B',
                    'start_date': T('2014-01-06'),
                    'end_date': T('2014-01-10'),
                    'exchange': "TEST",
                },

                # sid 1
                {
                    'symbol': 'C',
                    'asset_name': 'Asset C',
                    'start_date': T('2014-01-01'),
                    'end_date': T('2014-01-05'),
                    'exchange': "TEST",
                },
                {
                    'symbol': 'A',  # claiming the unused symbol 'A'
                    'asset_name': 'Asset A',
                    'start_date': T('2014-01-06'),
                    'end_date': T('2014-01-10'),
                    'exchange': "TEST",
                },
            ],
            index=[0, 0, 1, 1],
        )
        self.write_assets(equities=metadata)
        finder = self.asset_finder

        # note: these assertions walk forward in time, starting at assertions
        # about ownership before the start_date and ending with assertions
        # after the end_date; new assertions should be inserted in the correct
        # locations

        # no one held 'A' before 01
        with self.assertRaises(SymbolNotFound):
            finder.lookup_symbol('A', T('2013-12-31'))

        # no one held 'C' before 01
        with self.assertRaises(SymbolNotFound):
            finder.lookup_symbol('C', T('2013-12-31'))

        for asof in pd.date_range('2014-01-01', '2014-01-05', tz='utc'):
            # from 01 through 05 sid 0 held 'A'
            A_result = finder.lookup_symbol('A', asof)
            assert_equal(
                A_result,
                finder.retrieve_asset(0),
                msg=str(asof),
            )
            # The symbol and asset_name should always be the last held values
            assert_equal(A_result.symbol, 'B')
            assert_equal(A_result.asset_name, 'Asset B')

            # from 01 through 05 sid 1 held 'C'
            C_result = finder.lookup_symbol('C', asof)
            assert_equal(
                C_result,
                finder.retrieve_asset(1),
                msg=str(asof),
            )
            # The symbol and asset_name should always be the last held values
            assert_equal(C_result.symbol, 'A')
            assert_equal(C_result.asset_name, 'Asset A')

        # no one held 'B' before 06
        with self.assertRaises(SymbolNotFound):
            finder.lookup_symbol('B', T('2014-01-05'))

        # no one held 'C' after 06, however, no one has claimed it yet
        # so it still maps to sid 1
        assert_equal(
            finder.lookup_symbol('C', T('2014-01-07')),
            finder.retrieve_asset(1),
        )

        for asof in pd.date_range('2014-01-06', '2014-01-11', tz='utc'):
            # from 06 through 10 sid 0 held 'B'
            # we test through the 11th because sid 1 is the last to hold 'B'
            # so it should ffill
            B_result = finder.lookup_symbol('B', asof)
            assert_equal(
                B_result,
                finder.retrieve_asset(0),
                msg=str(asof),
            )
            assert_equal(B_result.symbol, 'B')
            assert_equal(B_result.asset_name, 'Asset B')

            # from 06 through 10 sid 1 held 'A'
            # we test through the 11th because sid 1 is the last to hold 'A'
            # so it should ffill
            A_result = finder.lookup_symbol('A', asof)
            assert_equal(
                A_result,
                finder.retrieve_asset(1),
                msg=str(asof),
            )
            assert_equal(A_result.symbol, 'A')
            assert_equal(A_result.asset_name, 'Asset A')

    def test_lookup_symbol(self):

        # Incrementing by two so that start and end dates for each
        # generated Asset don't overlap (each Asset's end_date is the
        # day after its start date.)
        dates = pd.date_range('2013-01-01', freq='2D', periods=5, tz='UTC')
        df = pd.DataFrame.from_records(
            [
                {
                    'sid': i,
                    'symbol':  'existing',
                    'start_date': date.value,
                    'end_date': (date + timedelta(days=1)).value,
                    'exchange': 'NYSE',
                }
                for i, date in enumerate(dates)
            ]
        )
        self.write_assets(equities=df)
        finder = self.asset_finder
        for _ in range(2):  # Run checks twice to test for caching bugs.
            with self.assertRaises(SymbolNotFound):
                finder.lookup_symbol('NON_EXISTING', dates[0])

            with self.assertRaises(MultipleSymbolsFound):
                finder.lookup_symbol('EXISTING', None)

            for i, date in enumerate(dates):
                # Verify that we correctly resolve multiple symbols using
                # the supplied date
                result = finder.lookup_symbol('EXISTING', date)
                self.assertEqual(result.symbol, 'EXISTING')
                self.assertEqual(result.sid, i)

    def test_fail_to_write_overlapping_data(self):
        df = pd.DataFrame.from_records(
            [
                {
                    'sid': 1,
                    'symbol': 'multiple',
                    'start_date': pd.Timestamp('2010-01-01'),
                    'end_date': pd.Timestamp('2012-01-01'),
                    'exchange': 'NYSE'
                },
                # Same as asset 1, but with a later end date.
                {
                    'sid': 2,
                    'symbol': 'multiple',
                    'start_date': pd.Timestamp('2010-01-01'),
                    'end_date': pd.Timestamp('2013-01-01'),
                    'exchange': 'NYSE'
                },
                # Same as asset 1, but with a later start_date
                {
                    'sid': 3,
                    'symbol': 'multiple',
                    'start_date': pd.Timestamp('2011-01-01'),
                    'end_date': pd.Timestamp('2012-01-01'),
                    'exchange': 'NYSE'
                },
            ]
        )

        with self.assertRaises(ValueError) as e:
            self.write_assets(equities=df)

        expected_error_msg = (
            "Ambiguous ownership for 1 symbol, multiple assets held the"
            " following symbols:\n"
            "MULTIPLE (??):\n"
            "  intersections: (('2010-01-01 00:00:00', '2012-01-01 00:00:00'),"
            " ('2011-01-01 00:00:00', '2012-01-01 00:00:00'))\n"
            "      start_date   end_date\n"
            "  sid                      \n"
            "  1   2010-01-01 2012-01-01\n"
            "  2   2010-01-01 2013-01-01\n"
            "  3   2011-01-01 2012-01-01"
        )
        self.assertEqual(str(e.exception), expected_error_msg)

    def test_lookup_generic(self):
        """
        Ensure that lookup_generic works with various permutations of inputs.
        """
        cases = build_lookup_generic_cases()
        # Make sure we clean up temp resources in the generator if we don't
        # consume the whole thing because of a failure.
        self.add_instance_callback(cases.close)
        for finder, inputs, reference_date, country, expected in cases:
            results, missing = finder.lookup_generic(
                inputs, reference_date, country,
            )
            self.assertEqual(results, expected)
            self.assertEqual(missing, [])

    def test_lookup_none_raises(self):
        """
        If lookup_symbol is vectorized across multiple symbols, and one of them
        is None, want to raise a TypeError.
        """

        with self.assertRaises(TypeError):
            self.asset_finder.lookup_symbol(None, pd.Timestamp('2013-01-01'))

    def test_lookup_mult_are_one(self):
        """
        Ensure that multiple symbols that return the same sid are collapsed to
        a single returned asset.
        """

        date = pd.Timestamp('2013-01-01', tz='UTC')

        df = pd.DataFrame.from_records(
            [
                {
                    'sid': 1,
                    'symbol': symbol,
                    'start_date': date.value,
                    'end_date': (date + timedelta(days=30)).value,
                    'exchange': 'NYSE',
                }
                for symbol in ('FOOB', 'FOO_B')
            ]
        )
        self.write_assets(equities=df)
        finder = self.asset_finder

        # If we are able to resolve this with any result, means that we did not
        # raise a MultipleSymbolError.
        result = finder.lookup_symbol('FOO/B', date + timedelta(1), fuzzy=True)
        self.assertEqual(result.sid, 1)

    def test_endless_multiple_resolves(self):
        """
        Situation:
        1. Asset 1 w/ symbol FOOB changes to FOO_B, and then is delisted.
        2. Asset 2 is listed with symbol FOO_B.

        If someone asks for FOO_B with fuzzy matching after 2 has been listed,
        they should be able to correctly get 2.
        """

        date = pd.Timestamp('2013-01-01', tz='UTC')

        df = pd.DataFrame.from_records(
            [
                {
                    'sid': 1,
                    'symbol': 'FOOB',
                    'start_date': date.value,
                    'end_date': date.max.value,
                    'exchange': 'NYSE',
                },
                {
                    'sid': 1,
                    'symbol': 'FOO_B',
                    'start_date': (date + timedelta(days=31)).value,
                    'end_date': (date + timedelta(days=60)).value,
                    'exchange': 'NYSE',
                },
                {
                    'sid': 2,
                    'symbol': 'FOO_B',
                    'start_date': (date + timedelta(days=61)).value,
                    'end_date': date.max.value,
                    'exchange': 'NYSE',
                },

            ]
        )
        self.write_assets(equities=df)
        finder = self.asset_finder

        # If we are able to resolve this with any result, means that we did not
        # raise a MultipleSymbolError.
        result = finder.lookup_symbol(
            'FOO/B',
            date + timedelta(days=90),
            fuzzy=True
        )
        self.assertEqual(result.sid, 2)

    def test_lookup_generic_handle_missing(self):
        data = pd.DataFrame.from_records(
            [
                {
                    'sid': 0,
                    'symbol': 'real',
                    'start_date': pd.Timestamp('2013-1-1', tz='UTC'),
                    'end_date': pd.Timestamp('2014-1-1', tz='UTC'),
                    'exchange': 'TEST',
                },
                {
                    'sid': 1,
                    'symbol': 'also_real',
                    'start_date': pd.Timestamp('2013-1-1', tz='UTC'),
                    'end_date': pd.Timestamp('2014-1-1', tz='UTC'),
                    'exchange': 'TEST',
                },
                # Sid whose end date is before our query date.  We should
                # still correctly find it.
                {
                    'sid': 2,
                    'symbol': 'real_but_old',
                    'start_date': pd.Timestamp('2002-1-1', tz='UTC'),
                    'end_date': pd.Timestamp('2003-1-1', tz='UTC'),
                    'exchange': 'TEST',
                },
                # Sid whose start_date is **after** our query date.  We should
                # **not** find it.
                {
                    'sid': 3,
                    'symbol': 'real_but_in_the_future',
                    'start_date': pd.Timestamp('2014-1-1', tz='UTC'),
                    'end_date': pd.Timestamp('2020-1-1', tz='UTC'),
                    'exchange': 'THE FUTURE',
                },
            ]
        )
        self.write_assets(equities=data)
        finder = self.asset_finder
        results, missing = finder.lookup_generic(
            ['REAL', 1, 'FAKE', 'REAL_BUT_OLD', 'REAL_BUT_IN_THE_FUTURE'],
            pd.Timestamp('2013-02-01', tz='UTC'),
            country_code=None,
        )

        self.assertEqual(len(results), 3)
        self.assertEqual(results[0].symbol, 'REAL')
        self.assertEqual(results[0].sid, 0)
        self.assertEqual(results[1].symbol, 'ALSO_REAL')
        self.assertEqual(results[1].sid, 1)
        self.assertEqual(results[2].symbol, 'REAL_BUT_OLD')
        self.assertEqual(results[2].sid, 2)

        self.assertEqual(len(missing), 2)
        self.assertEqual(missing[0], 'FAKE')
        self.assertEqual(missing[1], 'REAL_BUT_IN_THE_FUTURE')

    def test_lookup_generic_multiple_symbols_across_countries(self):
        data = pd.DataFrame.from_records(
            [
                {
                    'sid': 0,
                    'symbol': 'real',
                    'start_date': pd.Timestamp('2013-1-1', tz='UTC'),
                    'end_date': pd.Timestamp('2014-1-1', tz='UTC'),
                    'exchange': 'US_EXCHANGE',
                },
                {
                    'sid': 1,
                    'symbol': 'real',
                    'start_date': pd.Timestamp('2013-1-1', tz='UTC'),
                    'end_date': pd.Timestamp('2014-1-1', tz='UTC'),
                    'exchange': 'CA_EXCHANGE',
                },
            ]
        )
        exchanges = pd.DataFrame.from_records([
            {'exchange': 'US_EXCHANGE', 'country_code': 'US'},
            {'exchange': 'CA_EXCHANGE', 'country_code': 'CA'},
        ])

        self.write_assets(equities=data, exchanges=exchanges)

        # looking up a symbol shared by two assets across countries should
        # raise a SameSymbolUsedAcrossCountries if a country code is not passed
        with self.assertRaises(SameSymbolUsedAcrossCountries):
            self.asset_finder.lookup_generic(
                'real',
                as_of_date=pd.Timestamp('2014-1-1', tz='UTC'),
                country_code=None,
            )

        with self.assertRaises(SameSymbolUsedAcrossCountries):
            self.asset_finder.lookup_generic(
                'real',
                as_of_date=None,
                country_code=None,
            )

        matches, missing = self.asset_finder.lookup_generic(
            'real',
            as_of_date=pd.Timestamp('2014-1-1', tz='UTC'),
            country_code='US',
        )
        self.assertEqual([matches], [self.asset_finder.retrieve_asset(0)])
        self.assertEqual(missing, [])

        matches, missing = self.asset_finder.lookup_generic(
            'real',
            as_of_date=pd.Timestamp('2014-1-1', tz='UTC'),
            country_code='CA',
        )
        self.assertEqual([matches], [self.asset_finder.retrieve_asset(1)])
        self.assertEqual(missing, [])

    def test_security_dates_warning(self):

        # Build an asset with an end_date
        eq_end = pd.Timestamp('2012-01-01', tz='UTC')
        equity_asset = Equity(1, symbol="TESTEQ", end_date=eq_end,
                              exchange_info=ExchangeInfo("TEST", "TEST", "??"))

        # Catch all warnings
        with warnings.catch_warnings(record=True) as w:
            # Cause all warnings to always be triggered
            warnings.simplefilter("always")
            equity_asset.security_start_date
            equity_asset.security_end_date
            equity_asset.security_name
            # Verify the warning
            self.assertEqual(3, len(w))
            for warning in w:
                self.assertTrue(issubclass(warning.category,
                                           DeprecationWarning))

    def test_compute_lifetimes(self):
        assets_per_exchange = 4
        trading_day = self.trading_calendar.day
        first_start = pd.Timestamp('2015-04-01', tz='UTC')

        equities = pd.concat(
            [
                make_rotating_equity_info(
                    num_assets=assets_per_exchange,
                    first_start=first_start,
                    frequency=trading_day,
                    periods_between_starts=3,
                    asset_lifetime=5,
                    exchange=exchange,
                )
                for exchange in (
                    'US_EXCHANGE_1',
                    'US_EXCHANGE_2',
                    'CA_EXCHANGE',
                    'JP_EXCHANGE',
                )
            ],
            ignore_index=True,
        )
        # make every symbol unique
        equities['symbol'] = list(string.ascii_uppercase[:len(equities)])

        # shuffle up the sids so they are not contiguous per exchange
        sids = np.arange(len(equities))
        np.random.RandomState(1337).shuffle(sids)
        equities.index = sids
        permute_sid = dict(zip(sids, range(len(sids)))).__getitem__

        exchanges = pd.DataFrame.from_records([
            {'exchange': 'US_EXCHANGE_1', 'country_code': 'US'},
            {'exchange': 'US_EXCHANGE_2', 'country_code': 'US'},
            {'exchange': 'CA_EXCHANGE', 'country_code': 'CA'},
            {'exchange': 'JP_EXCHANGE', 'country_code': 'JP'},
        ])
        sids_by_country = {
            'US': equities.index[:2 * assets_per_exchange],
            'CA': equities.index[
                2 * assets_per_exchange:3 * assets_per_exchange
            ],
            'JP': equities.index[3 * assets_per_exchange:],
        }
        self.write_assets(equities=equities, exchanges=exchanges)
        finder = self.asset_finder

        all_dates = pd.date_range(
            start=first_start,
            end=equities.end_date.max(),
            freq=trading_day,
        )

        for dates in all_subindices(all_dates):
            expected_with_start_raw = full(
                shape=(len(dates), assets_per_exchange),
                fill_value=False,
                dtype=bool,
            )
            expected_no_start_raw = full(
                shape=(len(dates), assets_per_exchange),
                fill_value=False,
                dtype=bool,
            )

            for i, date in enumerate(dates):
                it = equities.iloc[:4][['start_date', 'end_date']].itertuples(
                    index=False,
                )
                for j, (start, end) in enumerate(it):
                    # This way of doing the checks is redundant, but very
                    # clear.
                    if start <= date <= end:
                        expected_with_start_raw[i, j] = True
                        if start < date:
                            expected_no_start_raw[i, j] = True

            for country_codes in powerset(exchanges.country_code.unique()):
                expected_sids = pd.Int64Index(sorted(concat(
                    sids_by_country[country_code]
                    for country_code in country_codes
                )))
                permuted_sids = [
                    sid for sid in sorted(expected_sids, key=permute_sid)
                ]
                tile_count = len(country_codes) + ('US' in country_codes)
                expected_with_start = pd.DataFrame(
                    data=np.tile(
                        expected_with_start_raw,
                        tile_count,
                    ),
                    index=dates,
                    columns=pd.Int64Index(permuted_sids),
                )
                result = finder.lifetimes(
                    dates,
                    include_start_date=True,
                    country_codes=country_codes,
                )
                assert_equal(result.columns, expected_sids)
                result = result[permuted_sids]
                assert_equal(result, expected_with_start)

                expected_no_start = pd.DataFrame(
                    data=np.tile(
                        expected_no_start_raw,
                        tile_count,
                    ),
                    index=dates,
                    columns=pd.Int64Index(permuted_sids),
                )
                result = finder.lifetimes(
                    dates,
                    include_start_date=False,
                    country_codes=country_codes,
                )
                assert_equal(result.columns, expected_sids)
                result = result[permuted_sids]
                assert_equal(result, expected_no_start)

    def test_sids(self):
        # Ensure that the sids property of the AssetFinder is functioning
        self.write_assets(equities=make_simple_equity_info(
            [0, 1, 2],
            pd.Timestamp('2014-01-01'),
            pd.Timestamp('2014-01-02'),
        ))
        self.assertEqual({0, 1, 2}, set(self.asset_finder.sids))

    def test_lookup_by_supplementary_field(self):
        equities = pd.DataFrame.from_records(
            [
                {
                    'sid': 0,
                    'symbol': 'A',
                    'start_date': pd.Timestamp('2013-1-1', tz='UTC'),
                    'end_date': pd.Timestamp('2014-1-1', tz='UTC'),
                    'exchange': 'TEST',
                },
                {
                    'sid': 1,
                    'symbol': 'B',
                    'start_date': pd.Timestamp('2013-1-1', tz='UTC'),
                    'end_date': pd.Timestamp('2014-1-1', tz='UTC'),
                    'exchange': 'TEST',
                },
                {
                    'sid': 2,
                    'symbol': 'C',
                    'start_date': pd.Timestamp('2013-7-1', tz='UTC'),
                    'end_date': pd.Timestamp('2014-1-1', tz='UTC'),
                    'exchange': 'TEST',
                },
            ]
        )

        equity_supplementary_mappings = pd.DataFrame.from_records(
            [
                {
                    'sid': 0,
                    'field': 'ALT_ID',
                    'value': '100000000',
                    'start_date': pd.Timestamp('2013-1-1', tz='UTC'),
                    'end_date': pd.Timestamp('2013-6-28', tz='UTC'),
                },
                {
                    'sid': 1,
                    'field': 'ALT_ID',
                    'value': '100000001',
                    'start_date': pd.Timestamp('2013-1-1', tz='UTC'),
                    'end_date': pd.Timestamp('2014-1-1', tz='UTC'),
                },
                {
                    'sid': 0,
                    'field': 'ALT_ID',
                    'value': '100000002',
                    'start_date': pd.Timestamp('2013-7-1', tz='UTC'),
                    'end_date': pd.Timestamp('2014-1-1', tz='UTC'),
                },
                {
                    'sid': 2,
                    'field': 'ALT_ID',
                    'value': '100000000',
                    'start_date': pd.Timestamp('2013-7-1', tz='UTC'),
                    'end_date': pd.Timestamp('2014-1-1', tz='UTC'),
                },
            ]
        )

        self.write_assets(
            equities=equities,
            equity_supplementary_mappings=equity_supplementary_mappings,
        )

        af = self.asset_finder

        # Before sid 0 has changed ALT_ID.
        dt = pd.Timestamp('2013-6-28', tz='UTC')

        asset_0 = af.lookup_by_supplementary_field('ALT_ID', '100000000', dt)
        self.assertEqual(asset_0.sid, 0)

        asset_1 = af.lookup_by_supplementary_field('ALT_ID', '100000001', dt)
        self.assertEqual(asset_1.sid, 1)

        # We don't know about this ALT_ID yet.
        with self.assertRaisesRegexp(
            ValueNotFoundForField,
            "Value '{}' was not found for field '{}'.".format(
                '100000002',
                'ALT_ID',
            )
        ):
            af.lookup_by_supplementary_field('ALT_ID', '100000002', dt)

        # After all assets have ended.
        dt = pd.Timestamp('2014-01-02', tz='UTC')

        asset_2 = af.lookup_by_supplementary_field('ALT_ID', '100000000', dt)
        self.assertEqual(asset_2.sid, 2)

        asset_1 = af.lookup_by_supplementary_field('ALT_ID', '100000001', dt)
        self.assertEqual(asset_1.sid, 1)

        asset_0 = af.lookup_by_supplementary_field('ALT_ID', '100000002', dt)
        self.assertEqual(asset_0.sid, 0)

        # At this point both sids 0 and 2 have held this value, so an
        # as_of_date is required.
        expected_in_repr = (
            "Multiple occurrences of the value '{}' found for field '{}'."
        ).format('100000000', 'ALT_ID')

        with self.assertRaisesRegexp(
            MultipleValuesFoundForField,
            expected_in_repr,
        ):
            af.lookup_by_supplementary_field('ALT_ID', '100000000', None)

    def test_get_supplementary_field(self):
        equities = pd.DataFrame.from_records(
            [
                {
                    'sid': 0,
                    'symbol': 'A',
                    'start_date': pd.Timestamp('2013-1-1', tz='UTC'),
                    'end_date': pd.Timestamp('2014-1-1', tz='UTC'),
                    'exchange': 'TEST',
                },
                {
                    'sid': 1,
                    'symbol': 'B',
                    'start_date': pd.Timestamp('2013-1-1', tz='UTC'),
                    'end_date': pd.Timestamp('2014-1-1', tz='UTC'),
                    'exchange': 'TEST',
                },
                {
                    'sid': 2,
                    'symbol': 'C',
                    'start_date': pd.Timestamp('2013-7-1', tz='UTC'),
                    'end_date': pd.Timestamp('2014-1-1', tz='UTC'),
                    'exchange': 'TEST',
                },
            ]
        )

        equity_supplementary_mappings = pd.DataFrame.from_records(
            [
                {
                    'sid': 0,
                    'field': 'ALT_ID',
                    'value': '100000000',
                    'start_date': pd.Timestamp('2013-1-1', tz='UTC'),
                    'end_date': pd.Timestamp('2013-6-28', tz='UTC'),
                },
                {
                    'sid': 1,
                    'field': 'ALT_ID',
                    'value': '100000001',
                    'start_date': pd.Timestamp('2013-1-1', tz='UTC'),
                    'end_date': pd.Timestamp('2014-1-1', tz='UTC'),
                },
                {
                    'sid': 0,
                    'field': 'ALT_ID',
                    'value': '100000002',
                    'start_date': pd.Timestamp('2013-7-1', tz='UTC'),
                    'end_date': pd.Timestamp('2014-1-1', tz='UTC'),
                },
                {
                    'sid': 2,
                    'field': 'ALT_ID',
                    'value': '100000000',
                    'start_date': pd.Timestamp('2013-7-1', tz='UTC'),
                    'end_date': pd.Timestamp('2014-1-1', tz='UTC'),
                },
            ]
        )

        self.write_assets(
            equities=equities,
            equity_supplementary_mappings=equity_supplementary_mappings,
        )
        finder = self.asset_finder

        # Before sid 0 has changed ALT_ID and sid 2 has started.
        dt = pd.Timestamp('2013-6-28', tz='UTC')

        for sid, expected in [(0, '100000000'), (1, '100000001')]:
            self.assertEqual(
                finder.get_supplementary_field(sid, 'ALT_ID', dt),
                expected,
            )

        # Since sid 2 has not yet started, we don't know about its
        # ALT_ID.
        with self.assertRaisesRegexp(
            NoValueForSid,
            "No '{}' value found for sid '{}'.".format('ALT_ID', 2),
        ):
            finder.get_supplementary_field(2, 'ALT_ID', dt),

        # After all assets have ended.
        dt = pd.Timestamp('2014-01-02', tz='UTC')

        for sid, expected in [
            (0, '100000002'), (1, '100000001'), (2, '100000000'),
        ]:
            self.assertEqual(
                finder.get_supplementary_field(sid, 'ALT_ID', dt),
                expected,
            )

        # Sid 0 has historically held two values for ALT_ID by this dt.
        with self.assertRaisesRegexp(
            MultipleValuesFoundForSid,
            "Multiple '{}' values found for sid '{}'.".format('ALT_ID', 0),
        ):
            finder.get_supplementary_field(0, 'ALT_ID', None),

    def test_group_by_type(self):
        equities = make_simple_equity_info(
            range(5),
            start_date=pd.Timestamp('2014-01-01'),
            end_date=pd.Timestamp('2015-01-01'),
        )
        futures = make_commodity_future_info(
            first_sid=6,
            root_symbols=['CL'],
            years=[2014],
        )
        # Intersecting sid queries, to exercise loading of partially-cached
        # results.
        queries = [
            ([0, 1, 3], [6, 7]),
            ([0, 2, 3], [7, 10]),
            (list(equities.index), list(futures.index)),
        ]
        self.write_assets(
            equities=equities,
            futures=futures,
        )
        finder = self.asset_finder
        for equity_sids, future_sids in queries:
            results = finder.group_by_type(equity_sids + future_sids)
            self.assertEqual(
                results,
                {'equity': set(equity_sids), 'future': set(future_sids)},
            )

    @parameterized.expand([
        (Equity, 'retrieve_equities', EquitiesNotFound),
        (Future, 'retrieve_futures_contracts', FutureContractsNotFound),
    ])
    def test_retrieve_specific_type(self, type_, lookup_name, failure_type):
        equities = make_simple_equity_info(
            range(5),
            start_date=pd.Timestamp('2014-01-01'),
            end_date=pd.Timestamp('2015-01-01'),
        )
        max_equity = equities.index.max()
        futures = make_commodity_future_info(
            first_sid=max_equity + 1,
            root_symbols=['CL'],
            years=[2014],
        )
        equity_sids = [0, 1]
        future_sids = [max_equity + 1, max_equity + 2, max_equity + 3]
        if type_ == Equity:
            success_sids = equity_sids
            fail_sids = future_sids
        else:
            fail_sids = equity_sids
            success_sids = future_sids

        self.write_assets(
            equities=equities,
            futures=futures,
        )
        finder = self.asset_finder
        # Run twice to exercise caching.
        lookup = getattr(finder, lookup_name)
        for _ in range(2):
            results = lookup(success_sids)
            self.assertIsInstance(results, dict)
            self.assertEqual(set(results.keys()), set(success_sids))
            self.assertEqual(
                valmap(int, results),
                dict(zip(success_sids, success_sids)),
            )
            self.assertEqual(
                {type_},
                {type(asset) for asset in itervalues(results)},
            )
            with self.assertRaises(failure_type):
                lookup(fail_sids)
            with self.assertRaises(failure_type):
                # Should fail if **any** of the assets are bad.
                lookup([success_sids[0], fail_sids[0]])

    def test_retrieve_all(self):
        equities = make_simple_equity_info(
            range(5),
            start_date=pd.Timestamp('2014-01-01'),
            end_date=pd.Timestamp('2015-01-01'),
        )
        max_equity = equities.index.max()
        futures = make_commodity_future_info(
            first_sid=max_equity + 1,
            root_symbols=['CL'],
            years=[2014],
        )
        self.write_assets(
            equities=equities,
            futures=futures,
        )
        finder = self.asset_finder
        all_sids = finder.sids
        self.assertEqual(len(all_sids), len(equities) + len(futures))
        queries = [
            # Empty Query.
            (),
            # Only Equities.
            tuple(equities.index[:2]),
            # Only Futures.
            tuple(futures.index[:3]),
            # Mixed, all cache misses.
            tuple(equities.index[2:]) + tuple(futures.index[3:]),
            # Mixed, all cache hits.
            tuple(equities.index[2:]) + tuple(futures.index[3:]),
            # Everything.
            all_sids,
            all_sids,
        ]
        for sids in queries:
            equity_sids = [i for i in sids if i <= max_equity]
            future_sids = [i for i in sids if i > max_equity]
            results = finder.retrieve_all(sids)
            self.assertEqual(sids, tuple(map(int, results)))

            self.assertEqual(
                [Equity for _ in equity_sids] +
                [Future for _ in future_sids],
                list(map(type, results)),
            )
            self.assertEqual(
                (
                    list(equities.symbol.loc[equity_sids]) +
                    list(futures.symbol.loc[future_sids])
                ),
                list(asset.symbol for asset in results),
            )

    @parameterized.expand([
        (EquitiesNotFound, 'equity', 'equities'),
        (FutureContractsNotFound, 'future contract', 'future contracts'),
        (SidsNotFound, 'asset', 'assets'),
    ])
    def test_error_message_plurality(self,
                                     error_type,
                                     singular,
                                     plural):
        try:
            raise error_type(sids=[1])
        except error_type as e:
            self.assertEqual(
                str(e),
                "No {singular} found for sid: 1.".format(singular=singular)
            )
        try:
            raise error_type(sids=[1, 2])
        except error_type as e:
            self.assertEqual(
                str(e),
                "No {plural} found for sids: [1, 2].".format(plural=plural)
            )