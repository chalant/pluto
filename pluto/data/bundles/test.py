import tarfile

from os import path

import pandas as pd

from zipline.assets import assets

from zipline.data.bundles import core
from zipline.data.minute_bars import BcolzMinuteBarReader
from zipline.data.adjustments import SQLiteAdjustmentReader

from zipline.data.bcolz_daily_bars import BcolzDailyBarReader

from zipline.testing import test_resource_path

from pluto.data.bundles.core import AbstractBundle


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
