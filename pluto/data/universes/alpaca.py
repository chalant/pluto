from abc import ABC

from pluto.data.universes import core
from pluto.data.bundles import pluto


class AlpacaUniverse(core.AbstractUniverse):
    def __init__(self, root_dir):
        super(AlpacaUniverse, self).__init__()
        self._bundle_data = alpaca.AlpacaBundleLoader(root_dir).load()

        self._equity_daily_bar_reader = self._equity_daily_bar_reader = self._bundle_data.equity_daily_bar_reader

    @property
    def start_date(self):
        return self._bundle_data.first_trading_day

    @property
    def first_trading_day(self):
        return self._equity_daily_bar_reader.first_trading_day

    @property
    def last_available_dt(self):
        return self._equity_daily_bar_reader.last_available_dt

    @property
    def asset_finder(self):
        return self._bundle_data.asset_finder

    @property
    def equity_minute_bar_reader(self):
        return self._bundle_data.equity_daily_bar_reader

    @property
    def equity_daily_bar_reader(self):
        return self._bundle_data.equity_daily_bar_reader

    @property
    def adjustment_reader(self):
        return self._bundle_data.adjustment_reader

    def pipeline_loader(self, domain):
        return

    def base_calendars(self):
        return ''

