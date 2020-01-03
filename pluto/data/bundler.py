from functools import partial

from zipline.data import bundles

from pluto.data.io import paths
from pluto.data import io

class Bundle(object):
    def __init__(self,
                 asset_finder,
                 equity_daily_bar_reader,
                 equity_minute_bar_reader,
                 adjustment_reader):
        self._asset_finder = asset_finder
        self._equity_daily_bar_reader = equity_daily_bar_reader
        self._equity_minute_bar_reader = equity_minute_bar_reader
        self._adjustment_reader = adjustment_reader

    @property
    def asset_finder(self):
        return self._asset_finder

    @property
    def equity_daily_bar_reader(self):
        return self._equity_daily_bar_reader

    @property
    def equity_minute_bar_reader(self):
        return self._equity_minute_bar_reader

    @property
    def adjustment_reader(self):
        return self._adjustment_reader


def get_bundle_loader(platform):
    '''creates a bundle object from the specified format'''

    if platform != 'pluto':
        #returns the zipline bundle load function
        return bundles.load
    else:

        def load(bundle_name='pluto'):
            daily_bars_reader = io.daily_bars_reader(paths.daily_bars(bundle_name))
            minute_bars_reader = io.minute_bars_reader(paths.minute_bars(bundle_name))
            adjustment_reader = io.adjustments_reader(paths.adjustments(bundle_name))
            asset_metadata = io.asset_finder(paths.assets(bundle_name))

            return Bundle(
                asset_metadata,
                daily_bars_reader,
                minute_bars_reader,
                adjustment_reader)
        return load