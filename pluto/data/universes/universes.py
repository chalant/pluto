import abc
import os

import sqlalchemy as sa

from dev import events

from zipline.data import bundles

from zipline.pipeline.loaders import USEquityPricingLoader
from zipline.pipeline.data import equity_pricing

from zipline.country import CountryCode

from pluto.data import benchmark as bm
from pluto.data.bundles import test

# from pluto.universes import writer

from pluto.trading_calendars import calendar_utils as cu

from protos import calendar_pb2

_CACHE = {}


class AbstractUniverse(abc.ABC):
    @property
    @abc.abstractmethod
    def country_code(self):
        return

    @property
    @abc.abstractmethod
    def start_date(self):
        return

    @property
    @abc.abstractmethod
    def end_date(self):
        return

    @property
    @abc.abstractmethod
    def domain(self):
        raise NotImplementedError(self.domain.__name__)

    @property
    @abc.abstractmethod
    def trading_calendar(self):
        raise NotImplementedError(self.calendar.__name__)

    @property
    @abc.abstractmethod
    def calendar_name(self):
        raise NotImplementedError(self.exchanges.__name__)

    @property
    @abc.abstractmethod
    def name(self):
        raise NotImplementedError(self.name.__name__)

    def load_calendar(self, start, end):
        """

        Parameters
        ----------
        start
        end

        Returns
        -------
        trading_calendars.TradingCalendar
        """
        raise NotImplementedError(self.load_calendar.__name__)

    @property
    @abc.abstractmethod
    def exchanges(self):
        raise NotImplementedError(self.exchanges.__name__)

    @property
    @abc.abstractmethod
    def bundle_name(self):
        raise NotImplementedError(self.bundle_name.__name__())

    @property
    @abc.abstractmethod
    def platform(self):
        raise NotImplementedError(self.platform.__name__)

    def load_bundle(self):
        bundles.load(self.bundle_name)

    @property
    @abc.abstractmethod
    def benchmark(self):
        raise NotImplementedError('benchmark')

    @property
    @abc.abstractmethod
    def base_calendars(self):
        # should return the calendar the universe is composed with...
        raise NotImplementedError

    @abc.abstractmethod
    def pipeline_loader(self, domain):
        raise NotImplementedError('pipeline_loader')

    @property
    @abc.abstractmethod
    def asset_finder(self):
        raise NotImplementedError('asset_finder')

    @property
    @abc.abstractmethod
    def equity_minute_bar_reader(self):
        raise NotImplementedError('equity_minute_bar_reader')

    @property
    @abc.abstractmethod
    def equity_daily_bar_reader(self):
        raise NotImplementedError('equity_daily_bar_reader')

    @property
    @abc.abstractmethod
    def adjustment_reader(self):
        raise NotImplementedError('')


class ZiplineQuandlUniverse(AbstractUniverse):
    # for testing purposes, it is not meant to be
    # used in production
    @property
    def name(self):
        return 'zipline_quandl'

    @property
    def exchanges(self):
        return 'AMEX', 'NASDAQ', 'NYSE'

    @property
    def calendars(self):
        return ('XNYS',)

    @property
    def platform(self):
        return 'zipline'

    @property
    def bundle_name(self):
        return 'quandl'

    @property
    def benchmark(self):
        return bm.ZiplineBenchmark()

    @property
    def pipeline_loader(self):
        raise NotImplementedError


class TestUniverse(AbstractUniverse):
    def __init__(self, root_dir, calendar_cache=False, environ=None):
        super(TestUniverse, self).__init__()

        self._calendar = None
        self._cache = calendar_cache

        self._bundle = test.TestBundle(root_dir)
        self._bundle_data = self._bundle.load()

        self._equity_daily_bar_reader = e = self._bundle_data.equity_daily_bar_reader

    @property
    def country_code(self):
        return CountryCode.UNITED_STATES

    @property
    def start_date(self):
        return self._equity_daily_bar_reader.first_trading_day

    @property
    def end_date(self):
        return self._equity_daily_bar_reader.last_available_dt

    @property
    def name(self):
        return 'test'

    @property
    def exchanges(self):
        return 'AMEX', 'NYSE', 'NASDAQ'

    @property
    def domain(self):
        return self._domain

    @property
    def calendar_name(self):
        return 'XNYS'

    @property
    def platform(self):
        return 'zipline'

    @property
    def bundle_name(self):
        return 'test'

    @property
    def data_frequencies(self):
        return 'daily',

    def load_bundle(self):
        return self._bundle_data

    @property
    def benchmark(self):
        return bm.ZiplineBenchmark(
            environ=self._bundle._environ)

    @property
    def trading_calendar(self):
        return self._calendar

    @property
    def base_calendars(self):
        return 'XNYS',

    def load_calendar(self, start, end):
        self._calendar = calendar = cu.get_calendar_in_range(
            'XNYS',
            start,
            end,
            self._cache)

        return calendar

    def pipeline_loader(self, domain):
        bundle = self._bundle_data

        loader = USEquityPricingLoader.without_fx(
            bundle.equity_daily_bar_reader,
            bundle.adjustment_reader
        )

        eq = equity_pricing.EquityPricing.specialize(domain)

        # for a single pipeline, we can't have multiple domains...

        def choose_loader(column):
            # todo: data_sets should can have "overlapping" columns
            # todo: we need more pipeline loaders (fundamentals etc.)
            # as-well as associated data-sets and domains
            if column in eq.columns:
                return loader
            raise ValueError(
                "No PipelineLoader registered for column %s." % column)

        return choose_loader

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


def with_cache(cache):
    def decorator(fn):
        def wrapper(name, cache_=False, environ=None):
            if name not in cache:
                uni = fn(name, cache_, environ)
                cache[name] = uni
                return uni

            return cache[name]

        return wrapper
    return decorator


@with_cache(_CACHE)
def get_universe(name, root_dir, cache=True, environ=None) -> AbstractUniverse:
    """

    Parameters
    ----------
    name: str
    root_dir: directory.directory.Directory
    cache:

    Returns
    -------
    AbstractUniverse
    """

    # cache universe in case we use it in-memory...

    if name == 'test':
        return TestUniverse(root_dir, cache, environ)
    elif name == 'quandl':
        return ZiplineQuandlUniverse()
    else:
        raise NotImplementedError("No universe named {}".format(name))
        # return Universe(name, writer.get_directory(name))

