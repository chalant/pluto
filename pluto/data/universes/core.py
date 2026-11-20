import abc


from trading_calendars import calendar_utils

from zipline.data import bundles

from zipline.pipeline.loaders import USEquityPricingLoader
from zipline.pipeline.data import equity_pricing

from zipline.country import CountryCode

from pluto.data import benchmark as bm
from pluto.data.bundles import test

from pluto.trading_calendars import calendar_utils as cu

_CACHE = {}


class AbstractUniverse(abc.ABC):
    @property
    @abc.abstractmethod
    def country_codes(self):
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
    def __init__(self,
                 root_dir,
                 data_cache,
                 calendar_cache=False,
                 environ=None):
        super(TestUniverse, self).__init__()

        self._calendar = None
        self._cache = calendar_cache

        self._bundle = bundle = test.TestBundleLoader(root_dir)

        self._bundle_data = bundle.load()

        self._equity_daily_bar_reader = self._bundle_data.equity_daily_bar_reader
        self._environ = environ if environ else bundle._environ

        calendar_utils._default_calendar_aliases['YAHOO'] = 'XNYS'

    @property
    def environ(self):
        return self._environ

    @property
    def country_codes(self):
        return CountryCode.UNITED_STATES,

    @property
    def country_code(self):
        return CountryCode.UNITED_STATES

    @property
    def start_date(self): 
        return self._equity_daily_bar_reader.first_trading_day

    @property
    def first_trading_day(self):
        return self._equity_daily_bar_reader.first_trading_day

    @property
    def last_available_dt(self):
        return self._equity_daily_bar_reader.last_available_dt

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
        return bm.ZiplineBenchmark(environ=self._environ)

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
        def wrapper(name, root_dir, *args, **kwargs):
            # one universe per (name, root directory): a universe keeps the
            # readers and environ of the directory it was loaded from.
            key = (name, getattr(root_dir, 'path', root_dir))
            if key not in cache:
                uni = fn(name, root_dir, *args, **kwargs)
                cache[key] = uni
                return uni

            return cache[key]

        return wrapper
    return decorator


@with_cache(_CACHE)
def get_universe(name, root_dir, data_cache, environ=None, calendar_cache=True) -> AbstractUniverse:
    """

    Parameters
    ----------
    name: str
    root_dir: directory.utils.DirectoryInterface
    cache: data_cache

    Returns
    -------
    AbstractUniverse
    """

    # cache universe in case we use it in-memory...

    if name == 'test':
        return TestUniverse(
            root_dir,
            data_cache,
            calendar_cache,
            environ)
    elif name == 'quandl':
        return ZiplineQuandlUniverse()
    else:
        raise NotImplementedError("No universe named {}".format(name))
        # return Universe(name, writer.get_directory(name))

