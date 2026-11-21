import os

import pandas as pd

import zipline

from pluto.interface.utils import paths


class Benchmark(object):
    def __init__(self, directory, ticker):
        self._dir = paths.get_dir(
            ticker,
            root=paths.get_dir('benchmark', paths.get_dir('data')))

    def get_history_window(self, start_dt, end_dt, frequency, ffill=True):
        #todo: use open to calculate return on the first day if we don't have enough data
        #
        # # get a minute history window of the first day
        # first_open = benchmark.get_spot_value(
        #     asset,
        #     'open',
        #     first_trading_day,
        #     'daily',
        # )
        # first_close = benchmark.get_spot_value(
        #     asset,
        #     'close',
        #     first_trading_day,
        #     'daily',
        # )
        #
        # first_day_return = (first_close - first_open) / first_open

        path = paths.get_file_path(frequency, self._dir)
        df = pd.read_csv(path)
        df['Date'] = df['Date'].astype('datetime64[ns]')
        df = df.set_index('Date')
        return df['Close'].ffill().pct_change(fill_method=None).iloc[1:][df.index.slice_indexer(start_dt, end_dt)]


class ZiplineBenchmark(object):
    """Daily returns of a ticker from a ``<ticker>_benchmark.csv`` file, the
    format of zipline's benchmark data. zipline-reloaded no longer downloads
    benchmark data (ensure_benchmark_data is gone): the file is read from
    ``<ZIPLINE_ROOT>/data`` when it exists there, otherwise from zipline's
    checked-in data (SPY only)."""

    def __init__(self, ticker='SPY', environ=None):
        self._environ = environ
        self._ticker = ticker

    def _path(self):
        name = '{}_benchmark.csv'.format(self._ticker)
        root = (self._environ or os.environ).get('ZIPLINE_ROOT')
        if root:
            path = os.path.join(root, 'data', name)
            if os.path.exists(path):
                return path
        return os.path.join(os.path.dirname(zipline.__file__), 'resources', 'market_data', name)

    def read(self):
        path = self._path()
        with open(path) as f:
            has_header = f.readline().lower().startswith('date')
        df = pd.read_csv(
            path,
            header=0 if has_header else None,
            names=None if has_header else ['date', 'return'],
            index_col='date',
            parse_dates=['date'],
            # exact decimal to double conversion, as the stored results (pandas
            # 0.22) and independent of the pandas default parser
            float_precision='round_trip')
        returns = df['return'].sort_index()
        if returns.index.tz is None:
            returns.index = returns.index.tz_localize('UTC')
        return returns

    def get_history_window(self, start_dt, end_dt, frequency, ffill=True):
        br = self.read()
        start_dt, end_dt = (pd.Timestamp(dt) for dt in (start_dt, end_dt))
        if start_dt.tz is None:
            start_dt = start_dt.tz_localize('UTC')
        if end_dt.tz is None:
            end_dt = end_dt.tz_localize('UTC')
        return br[br.index.slice_indexer(start_dt, end_dt)].iloc[:]