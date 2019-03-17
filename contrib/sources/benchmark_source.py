import pandas as pd


class BenchmarkSource(object):
    def __init__(self, benchmark_asset, emission_rate="daily"):
        self._benchmark_asset = benchmark_asset
        self._emission_rate = emission_rate

        self._minutes = pd.DatetimeIndex([])
        self._days = pd.DatetimeIndex([])

        self._precalculated_series = None
        self._daily_returns = None


    def on_session_end(self, dt, data_portal, trading_calendar):
        if self._emission_rate == 'daily':
            self._days.append(dt)
            self._precalculated_series, self._daily_returns = self._calculate_benchmark_series(
                self._benchmark_asset, data_portal, trading_calendar, 'daily'
            )

    def on_minute_end(self, dt, data_portal, trading_calendar):
        self._minutes.append(dt)
        self._precalculated_series, self._daily_returns = self._calculate_benchmark_series(
            self._benchmark_asset, data_portal, trading_calendar, 'minute')

    def daily_returns(self, start=None, end=None):
        if start is None:
            if end is None:
                return self._daily_returns
            return self._daily_returns[:end]
        else:
            if end is None:
                return self._daily_returns[start:]
            return self._daily_returns[start:end]

    def get_range(self, start_dt, end_dt):
        return self._precalculated_series.loc[start_dt:end_dt]

    def _calculate_benchmark_series(self, asset, data_portal, trading_calendar, emission_rate):
        minutes = self._minutes
        if emission_rate == "minute":
            benchmark_series = data_portal.get_history_window(
                [asset],
                minutes[-1],
                bar_count=len(minutes) + 1,
                frequency="1m",
                field="price",
                data_frequency=emission_rate,
                ffill=True
            )[asset]

            return (
                benchmark_series.pct_change()[1:],
                self.downsample_minute_return_series(trading_calendar, benchmark_series))

        start_date = asset.start_date

        trading_days = self._days
        first_trading_day = trading_days[0]
        last_trading_day = trading_days[-1]

        if start_date < first_trading_day:
            # get the window of close prices for benchmark_asset from the
            # last trading day of the simulation, going up to one day
            # before the simulation start day (so that we can get the %
            # change on day 1)
            benchmark_series = data_portal.get_history_window(
                [asset],
                last_trading_day,
                bar_count=len(trading_days) + 1,
                frequency="1d",
                field="price",
                data_frequency=emission_rate,
                ffill=True
            )[asset]

            returns = benchmark_series.pct_change()[1:]
            return returns, returns
        elif start_date == first_trading_day:
            # Attempt to handle case where stock data starts on first
            # day, in this case use the open to close return.
            benchmark_series = data_portal.get_history_window(
                [asset],
                last_trading_day,
                bar_count=len(trading_days),
                frequency="1d",
                field="price",
                data_frequency=emission_rate,
                ffill=True
            )[asset]

            # get a minute history window of the first day
            first_open = data_portal.get_spot_value(
                asset,
                'open',
                first_trading_day,
                'daily',
            )
            first_close = data_portal.get_spot_value(
                asset,
                'close',
                first_trading_day,
                'daily',
            )

            first_day_return = (first_close - first_open) / first_open

            returns = benchmark_series.pct_change()[:]
            returns[0] = first_day_return
            return returns, returns
        else:
            raise ValueError(
                'cannot set benchmark to asset that does not exist during'
                ' the simulation period (asset start date=%r)' % start_date
            )

    @classmethod
    def downsample_minute_return_series(cls, trading_calendar, minutely_returns):
        sessions = trading_calendar.minute_index_to_session_labels(
            minutely_returns.index,
        )
        closes = trading_calendar.session_closes_in_range(
            sessions[0],
            sessions[-1],
        )
        daily_returns = minutely_returns[closes].pct_change()
        daily_returns.index = closes.index
        return daily_returns.iloc[1:]

    def get_value(self, dt):
        return self._precalculated_series.loc[dt]

