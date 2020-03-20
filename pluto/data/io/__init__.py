from zipline.data import (
    adjustments,
    hdf5_daily_bars)

from pluto.assets import assets
from pluto.data.io import daily_bars
from pluto.data.io import paths

#todo: we need standard paths for reading the files

def unsupported_error(func, format):
    raise ValueError('Format {} is not supported for {}'.format(format, func.__name__))

def asset_finder(file_path):
    return assets.AssetFinder(file_path)

def minute_bars_reader(file_path):
    raise NotImplementedError(minute_bars_reader.__name__)

def adjustments_reader(file_path):
    return adjustments.SQLiteAdjustmentReader(file_path)

def daily_bars_reader(file_path):
    return hdf5_daily_bars.HDF5DailyBarReader(file_path)