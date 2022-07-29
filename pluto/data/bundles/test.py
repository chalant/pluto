import tarfile

from os import path

import pandas as pd

from zipline.assets import assets

from zipline.data.bundles import core
from zipline.data.minute_bars import BcolzMinuteBarReader
from zipline.data.adjustments import SQLiteAdjustmentReader

from zipline.data.bcolz_daily_bars import BcolzDailyBarReader

from zipline.testing import test_resource_path

from pluto.trading_calendars import calendar_utils
from pluto.data.bundles.core import AbstractBundle
from pluto.data.equity_pricing.daily_bar_writer import ClickHouseDailyBarWriter
from pluto.data.equity_pricing import readers
from pluto.settings import settings


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
    def __init__(self, root_dir, environ=None):
        super(ClickHouseTestBundle, self).__init__(
            'click_house_test',
            root_dir,
            'NYSE')

        self._environ = environ if environ else {}

        self._environ.setdefault(
            'ZIPLINE_ROOT',
            path.join(
                self._root.path,
                'example_data/root'))

        core.register('click_house_test', lambda *args: None)

    def _ingest(self, root_dir):
        timestamp = pd.Timestamp.utcnow()

        environ = self._environ
        name = 'test'

        with tarfile.open(test_resource_path('example_data.tar.gz')) as tar:
            tar.extractall(root_dir.path)

        timestr = core.most_recent_data(
            name,
            timestamp,
            environ=environ)

        asset_finder = assets.AssetFinder(
            core.asset_db_path(
                name,
                timestr,
                environ=environ),
        )

        daily_reader = BcolzDailyBarReader(
            core.daily_equity_path(
                name,
                timestr,
                environ=environ))

        writer = ClickHouseDailyBarWriter(
            settings['clickhouse_address']
        )

        cal = calendar_utils.get_calendar_in_range(
            'XNYS',
            daily_reader.first_trading_day,
            daily_reader.last_available_dt
        )

        minutes_in_range = cal.minutes_for_sessions_in_range(
            daily_reader.first_trading_day,
            daily_reader.last_available_dt
        )

        def generator():
            # insert each sid at a time
            for sid in asset_finder.sids:
                data = daily_reader.load_raw_arrays(
                    ['open', 'high', 'low', 'close', 'volume'],
                    daily_reader.first_trading_day,
                    daily_reader.last_available_dt,
                    sid)

                idx = 0

                for minute in minutes_in_range:
                    yield {
                        'open': data[0][idx, 0],
                        'high': data[1][idx, 0],
                        'low': data[2][idx, 0],
                        'close': data[3][idx, 0],
                        'volume': data[4][idx, 0],
                        'sid': sid,
                        'day': minute
                    }

                    idx += 1

        writer.write(generator())

    def _load(self, name):
        timestamp = pd.Timestamp.utcnow()
        environ = self._environ

        timestr = core.most_recent_data(name, timestamp, environ=environ)

        return core.BundleData(
            asset_finder=assets.AssetFinder(
                core.asset_db_path(name, timestr, environ=environ),
            ),
            equity_minute_bar_reader=readers.ClickHouseMinuteBarReader,
            equity_daily_bar_reader=readers.ClickHouseDailyBarReader(

            ),
            adjustment_reader=SQLiteAdjustmentReader(
                core.adjustment_db_path(name, timestr, environ=environ),
            )
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        core.unregister('click_house_test')
