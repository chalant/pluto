import pandas as pd

from zipline.data.loader import ensure_benchmark_data

from pluto.interface.utils import paths

class Benchmark(object):
    def __init__(self, ticker):
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
        return df['Close'][df.index.slice_indexer(start_dt, end_dt)].iloc[:]

class ZiplineBenchmark(object):
    def __init__(self, ticker='SPY', environ=None):
        self._environ = environ
        self._ticker = ticker

    def get_history_window(self, start_dt, end_dt, frequency, ffill=True):
        br = ensure_benchmark_data(
            self._ticker,
            start_dt,
            end_dt,
            pd.Timestamp.utcnow(),
            # We need the trading_day to figure out the close prior to the first
            # date so that we can compute returns for the first date.
            end_dt,
            self._environ,
        )
        return br[br.index.slice_indexer(start_dt, end_dt)].iloc[:]