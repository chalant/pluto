from pluto.interface.utils import paths
import pandas as pd

DIR = paths.get_dir('benchmark', paths.get_dir('data'))

class Benchmark(object):
    def __init__(self, ticker):
        self._dir = paths.get_dir(ticker, root=DIR)

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

