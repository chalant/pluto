import abc
import os

import sqlalchemy as sa

from zipline.data import bundles

from zipline.pipeline.loaders import USEquityPricingLoader
from zipline.pipeline.data import equity_pricing

from pluto.data import benchmark as bm

# from pluto.universes import writer

from pluto.trading_calendars import calendar_utils as cu

from pluto.pipeline import domain

from protos import calendar_pb2


class AbstractUniverse(abc.ABC):
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
    def calendars(self):
        raise NotImplementedError

    @abc.abstractmethod
    def pipeline_loader(self, bundle):
        raise NotImplementedError('pipeline_loader')


# class Universe(AbstractUniverse):
#     def __init__(self, name, directory):
#         self._name = name
#         self._directory = directory
#         self._exchanges = None
#         self._asset_filter = None
#         self._calendar_path = None
#
#         self._engine = writer.get_engine()
#
#         self._benchmark = None
#
#     @property
#     def name(self):
#         return self._name
#
#     @property
#     def exchanges(self):
#         # query the exchanges associated with this universe
#         exchanges = self._exchanges
#         if not exchanges:
#             with self._engine.begin() as conn:
#                 stm = schema.universes
#                 self._exchanges = \
#                     exchanges = [
#                     row[0]
#                     for row in conn.execute(
#                         sa.select([stm.c.exchange])
#                             .join(schema.universe_exchanges)
#                             .where(stm.c.universe == self._name))]
#         return exchanges
#
#     def get_calendar(self, start, end, cache=False):
#         '''
#
#         Parameters
#         ----------
#         start
#         end
#
#         Returns
#         -------
#         trading_calendars.TradingCalendar
#         '''
#         with self._engine.begin() as conn:
#             stm = schema.calendars
#             result = conn.execute(
#                 sa.select([stm.c.file_path])
#                     .join(schema.calendar_exchanges)
#                     .join(schema.exchanges)
#                     .where(*(stm.c.exchange == exchange
#                              for exchange in self._exchanges)))
#             self._calendar_path = cal_path = result.fetchone()['file_path']
#
#         with open(cal_path) as f:
#             proto = calendar_pb2.Calendar()
#             proto.ParseFromString(f.read())
#
#         # todo: the calendar should have a name.
#         return cu.from_proto_calendar(proto, start, end, cache)
#
#     @property
#     def calendars(self):
#         raise NotImplementedError
#
#     @property
#     def calendar_name(self):
#         raise NotImplementedError
#
#     @property
#     def bundle_name(self):
#         bundle = self._bundle_name
#         if not bundle:
#             with self._engine.begin() as conn:
#                 stm = schema.universes
#                 result = conn.execute(
#                     sa.select([stm.c.bundle])
#                         .where(self._name == stm.c.universe))
#                 self._bundle_name = bundle = result.fetchone()['bundle']
#         return bundle
#
#     @property
#     def asset_filter(self):
#         # returns an asset filter of this universe
#         asset_filter = self._asset_filter
#         if not asset_filter:
#             return AssetFilter(self._directory)
#         else:
#             return asset_filter
#
#     @property
#     def platform(self):
#         return 'pluto'
#
#     def pipeline_loader(self, bundle):
#         raise NotImplementedError
#
#     def benchmark(self):
#         if not self._benchmark:
#             self._benchmark = bm.Benchmark('SPY')
#         return self._benchmark


class ZiplineQuandlUniverse(AbstractUniverse):
    # for testing purposes, it is not meant to be
    # used in production
    @property
    def name(self):
        return 'zipline_quandl'

    @property
    def exchanges(self):
        return ('AMEX', 'NASDAQ', 'NYSE')

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

    def pipeline_loader(self, bundle):
        raise NotImplementedError


class TestUniverse(AbstractUniverse):
    def __init__(self, calendar_cache=False):
        super(TestUniverse, self).__init__()

        self._domain = domain.Domain(self)

        self._calendar = None
        self._cache = calendar_cache

    @property
    def name(self):
        return 'test'

    @property
    def exchanges(self):
        return {'AMEX', 'NYSE', 'NASDAQ'}

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

    def load_bundle(self):
        return bundles.load(
            self.name,
            environ=os.environ)

    @property
    def benchmark(self):
        return bm.ZiplineBenchmark(
            environ=os.environ)

    @property
    def trading_calendar(self):
        return self._calendar

    @property
    def calendars(self):
        return 'XNYS',

    def load_calendar(self, start, end):
        self._calendar = calendar = cu.get_calendar_in_range(
            'XNYS',
            start,
            end,
            self._cache)

        return calendar

    def pipeline_loader(self, bundle):
        loader = USEquityPricingLoader.without_fx(
            bundle.equity_daily_bar_reader,
            bundle.adjustment_reader
        )

        eq = equity_pricing.EquityPricing.specialize(self._domain)

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


def get_universe(name):
    '''

    Parameters
    ----------
    name: str

    Returns
    -------
    AbstractUniverse
    '''
    if name == 'test':
        return TestUniverse()
    elif name == 'quandl':
        return ZiplineQuandlUniverse()
    else:
        raise NotImplementedError("No universe named {}".format(name))
        # return Universe(name, writer.get_directory(name))
