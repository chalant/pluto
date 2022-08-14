import os.path
import tarfile

import numpy as np
from testfixtures import tempdirectory

import json

from os import path

from collections import defaultdict

import shutil

import pandas as pd

import clickhouse_driver

from zipline.assets import assets

from zipline.data.bundles import core
from zipline.data.minute_bars import BcolzMinuteBarReader
from zipline.data.adjustments import SQLiteAdjustmentReader

from zipline.data.bcolz_daily_bars import BcolzDailyBarReader

from zipline.testing import test_resource_path

from pluto.trading_calendars import calendar_utils
from pluto.data.bundles.core import AbstractBundle
from pluto.data.equity_pricing.daily_bar_writer import ClickHouseDailyBarWriter
from pluto.data.equity_pricing import daily_bar_writer as dbw
from pluto.data.equity_pricing import readers
from pluto.configuration import configuration


class TestBundle(AbstractBundle):
    def __init__(self, root_dir, environ=None):
        super(TestBundle, self).__init__(
            'test',
            root_dir,
            'NYSE')

        self._environ = environ if environ else {}

        self._environ.setdefault(
            'ZIPLINE_ROOT',
            path.join(
                self._root.path,
                'example_data/root'))

        core.register('test', lambda *args: None)

    def _ingest(self, root_dir):
        with tarfile.open(test_resource_path('example_data.tar.gz')) as tar:
            tar.extractall(root_dir.path)

    def _load(self, name):
        timestamp = pd.Timestamp.utcnow()
        environ = self._environ

        timestr = core.most_recent_data(name, timestamp, environ=environ)

        return core.BundleData(
            asset_finder=assets.AssetFinder(
                core.asset_db_path(name, timestr, environ=environ),
            ),
            equity_minute_bar_reader=BcolzMinuteBarReader(
                core.minute_equity_path(name, timestr, environ=environ),
            ),
            equity_daily_bar_reader=BcolzDailyBarReader(
                core.daily_equity_path(name, timestr, environ=environ),
            ),
            adjustment_reader=SQLiteAdjustmentReader(
                core.adjustment_db_path(name, timestr, environ=environ),
            )
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        core.unregister('test')


class ClickHouseTestBundle(AbstractBundle):
    def __init__(self, root_dir, cache, environ=None):
        super(ClickHouseTestBundle, self).__init__(
            'click_house_test',
            root_dir,
            'XNYS')

        self._environ = environ if environ else {}

        self._environ.setdefault(
            'ZIPLINE_ROOT',
            path.join(
                root_dir.path,
                'example_data/root'))

        db_config = configuration['clickhouse']

        with clickhouse_driver.Client(
            db_config['host'],
            db_config['port'],
            password=db_config['password']
        ) as cl:

            cl.execute(
                '''
                CREATE DATABASE IF NOT EXISTS test
                '''
            )

        self._db_config = db_config

        self._meta = self._root.add_file('metadata', 'json')
        self._lifetimes = self._root.add_file('lifetimes', 'json')
        self._cache = cache

        core.register('click_house_test', lambda *args: None)

    def _ingest(self, root_dir):
        # writes example data into clickhouse database
        timestamp = pd.Timestamp.utcnow()

        name = 'test'

        core.register('test', lambda *args: None)
        db_config = self._db_config

        with clickhouse_driver.Client(
                db_config['host'],
                db_config['port'],
                password=db_config['password']) as cl:

            # delete any previous table
            dbw.drop_table(
                cl,
                name
            )

        tmp = tempdirectory.TempDirectory(
            'example_data')

        with tarfile.open(test_resource_path('example_data.tar.gz')) as tar:
            tar.extractall(tmp.path)

        environ = {
            'ZIPLINE_ROOT': tmp.getpath('example_data/root')
        }

        timestr = core.most_recent_data(
            'test',
            timestamp,
            environ=environ)

        assets_path = path.join(root_dir.path, 'assets-7.sqlite')
        adjustment_path = path.join(root_dir.path, 'adjustments.sqlite')

        # copy assets database
        shutil.copy2(
            core.asset_db_path(name, timestr, environ),
            assets_path
        )

        # copy adjustment database
        shutil.copy2(core.adjustment_db_path(
            name,
            timestr,
            environ), adjustment_path)

        asset_finder = assets.AssetFinder(assets_path)

        daily_reader = BcolzDailyBarReader(
            core.daily_equity_path(
                name,
                timestr,
                environ=environ))

        writer = ClickHouseDailyBarWriter(
            db_config,
            name)

        cal = calendar_utils.get_calendar_in_range(
            'XNYS',
            daily_reader.first_trading_day,
            daily_reader.last_available_dt
        )

        # minutes_in_range = cal.minutes_for_sessions_in_range(
        #     daily_reader.first_trading_day,
        #     daily_reader.last_available_dt)

        sessions_in_range = cal.sessions_in_range(
            daily_reader.first_trading_day,
            daily_reader.last_available_dt)

        first_rows = daily_reader._first_rows
        last_rows = daily_reader._last_rows

        lifetimes = {}

        def convert_to_int(value, factor):
            if not np.isnan(value):
                return int(value * factor)
            return value

        def generator():
            # insert each sid at a time
            for sid in asset_finder.sids:
                data = daily_reader.load_raw_arrays(
                    ['open', 'high', 'low', 'close', 'volume'],
                    daily_reader.first_trading_day,
                    daily_reader.last_available_dt,
                    [sid,])

                idx = 0

                start_idx = first_rows[sid]
                end_idx = last_rows[sid]

                arr = [0, 0]

                lifetimes[sid] = arr

                for session in sessions_in_range:
                    volume = data[4][idx, 0]
                    close = data[3][idx, 0]

                    if idx == start_idx:
                        arr[0] = session.value

                    # fixme: should we consider zero volume
                    # as last traded day? (it is not always the case...)

                    if idx == end_idx:
                        arr[1] = session.value

                    if not np.isnan(close):
                        yield {
                            'open': convert_to_int(data[0][idx, 0], 1000),
                            'high': convert_to_int(data[1][idx, 0], 1000),
                            'low': convert_to_int(data[2][idx, 0], 1000),
                            'close': convert_to_int(close, 1000),
                            'volume': volume,
                            'sid': sid,
                            'day': session
                        }

                    idx += 1

        writer.write(generator())

        # write additional data

        with open(self._meta.path, 'w+') as f:
            json.dump({
                'first_trading_day': daily_reader.first_trading_day.isoformat(),
                'last_available_dt': daily_reader.last_available_dt.isoformat()}, f)

        with open(self._lifetimes.path, 'w+') as f:
            json.dump(lifetimes, f)

        # remove directory
        tmp.cleanup()

        print("Done Ingesting...")



    def _load(self, name):
        with open(self._meta.path) as f:
            bundle_meta = json.load(f)

        with open(self._lifetimes.path) as f:
            lifetimes = json.load(f)

        # load calendar...
        cal = calendar_utils.get_calendar_in_range(
            'XNYS',
            pd.Timestamp(bundle_meta['first_trading_day']),
            pd.Timestamp(bundle_meta['last_available_dt'])
        )

        root_dir = self._root

        return core.BundleData(
            asset_finder=assets.AssetFinder(path.join(root_dir.path, 'assets-7.sqlite')),
            equity_minute_bar_reader=readers.ClickHouseMinuteBarReader(),
            equity_daily_bar_reader=readers.ClickHouseDailyBarReader(
                self._db_config, cal,
                bundle_meta, lifetimes,
                self._cache, 'test'),
            adjustment_reader=SQLiteAdjustmentReader(path.join(root_dir.path, 'adjustments.sqlite')))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # todo: clear all the data
        # dbw.drop_table(self._client, 'test')
        core.unregister('click_house_test')
