import math
import json
import asyncio

import clickhouse_driver
import pandas

from pandas import Timestamp, Timedelta

from sqlalchemy import engine, text

from alpaca_trade_api import rest
import alpaca_trade_api as alp

from pluto.data.bundles import core
from pluto.data.equity_pricing import minute_bars_writer as mbw
from pluto.data.ingestion import eodhd_client
from pluto.trading_calendars import calendar_utils

from pluto.configuration import configuration

# todo: this class should also generate pipeline modules in the
#  .pluto directory for pipeline we will then append modules in the sys.path


class PlutoDataIngestion(object):
    def __init__(
            self,
            root_dir,
            start_dt: pandas.Timestamp,
            end_dt: pandas.Timestamp):

        self._start_dt = start_dt
        self._end_dt = end_dt

        db_config = configuration['clickhouse']
        alpaca_credentials = configuration['alpaca']
        polygon_credentials = configuration['polygon.io']
        self._eodhd_api_token = configuration['eodhd']['api_token']

        self._root_dir = root = root_dir.get_directory('data').get_directory('pluto')
        state_file = root.add_file('state.json')

        assets = root.add_file('assets.sqlite')
        self._db_engine = engine.create_engine('sqlite://{}'.format(assets.path))

        try:
            state_file.touch()

            with open(state_file.path()) as f:
                json.dump({
                    'initialized': False,
                    'last_ingested': None
                }, f)

        except FileExistsError:
            pass

        with open(state_file.path()) as f:
            self._ingestion_state = json.load(f)

        self._exclude = set(exclude)

        self._alpaca_client = rest.REST(
            alpaca_credentials['key_id'],
            alpaca_credentials['secret_key'])

        # todo: pluto client

        with clickhouse_driver.Client(
                db_config['host'],
                db_config['port'],
                password=db_config['password']) as cl:

            cl.execute('''CREATE DATABASE IF NOT EXISTS pluto_data''')

        with clickhouse_driver.Client(
                db_config['host'],
                db_config['port'],
                password=db_config['password'],
                database='pluto_data') as cl:

            cl.execute('''CREATE TABLE IF NOT EXISTS lifetimes''')
            cl.execute('''CREATE TABLE IF NOT EXISTS asset_symbols''')

        self._db_config = db_config

        self._writers = {}
        self._calendars = {}

    def append_bars(self, ts, data_source, calendar):
        writer = self._writers[calendar.name]

    def ingest(self, progress):
        # exchanges to download for...
        eodhd_token = self._eodhd_api_token

        ingestion_state = self._ingestion_state

        initialized = ingestion_state['initialized']
        last_ingestion = ingestion_state['last_ingestion']

        if not initialized:
            # list of exchanges (these contain different assets and exchanges)
            exchanges = ["US", "FOREX", "CC"]

            # initial write.

            # download from eodhd

            # todo: we need to map exchanges to calendars...
            #  US: XNYS, FOREX: 24/7h CC: 24/7

            # todo: once everything is ingested, further manual mapping
            #  in the table must be performed, since we don't always know which
            #  exchanges map to which calendars...

            # todo: memory map symbols?

            # todo: map asset types to exchanges.

            # todo: we need to map the exchanges to our internal calendars
            # todo: this could be done through some cli-tool and a file such as json or toml for mappings...

            symbols = eodhd_client.get_exchange_symbols(eodhd_token, "US")

            # split into k groups (say 10)
            # create writer for each group
            writer = self._writers['XNYS']

            # todo: pre-store sid to symbol mappings
            #  note: since the symbol changes, it is not a primary key
            #  we will be holding a history of symbols mapped to the sid
            #  in order to track any symbol changes and map it back to that sid.

            #  traverse the whole list to build the exchange database
            with self._db_engine.connect() as connection:
                # todo : we need to map exchanges to asset types

                # store exchanges
                connection.execute(
                    '''
                    INSERT INTO exchanges (exchange, canonical_name, country_code, currency) 
                    VALUES (?, ?, ?, ?)''',
                    self._exchange_data_generator(symbols))

                # insert assets
                connection.execute(
                    '''
                    INSERT INTO equity_assets (sid, symbol, asset_type, exchange)
                    VALUES (?, ?, ?, ?)
                    ''',
                    self._symbol_data_generator(symbols))

                # todo: for US, map exchange to calendar (XNYS)

            # todo: download stock splits and dividends
            #  these should be stored in a click-house data-base

            # todo: make a generator that splits requests into 120 days intervals...

            # 1 min only for NYSE and NASDAQ
            # max from to: 120 days for 1-minute intervals, 600 days for 5-minute intervals and
            # 7200 days for 1-hour intervals.

            # request 3 years worth of data...

            # todo: we assume that nothing is returned when there is no data for
            #  a certain request. maybe its a "not found error"...

            # todo: from ingested stocks, query symbols based on exchanges...
            #  problem: we do not know for which asset we have which frequency

            # ingestion for all XNYS stocks
            # todo: get last trading session...
            end = Timestamp.utcnow()
            start = end - Timedelta(years=3)
            cal = calendar_utils.get_calendar_in_range('NYSE', start, end)
            sessions = cal.sessions_in_range(start, end)

            num_sessions = len(sessions)
            # step depend on frequency...
            step = math.ceil(num_sessions/120)

            # todo: iterate every 120 days (for minute)
            with self._db_engine.connect() as connection:
                to_download = [res[0] for res in connection.execute(
                    '''
                    SELECT symbol FROM equity_assets 
                    WHERE exchange=NYSE AND exchange=NASDAQ''')]

                progress.target = len(to_download)

                # example for splitting progress into sub-progress
                #  of 10 elements...
                p1 = progress.split(10)

                for symbol in to_download:
                    for i in range(0, num_sessions - step):
                        sess = sessions[i, i + step]
                        res = eodhd_client.get_historical_data(
                            eodhd_token,
                            symbol[0], '1m',
                            sess[0], sess[-1])

                        progress.increment()

            # todo: download starting from start_dt to utc_now for each symbol
            #  use multiple threads to make it faster. store in folders and multiple files for each thread.

            # todo: store data frequency mappings with each symbol...
            # todo: since we don't know for which ticker we have data for a certain frequency,
            #  if a request returns nothing then there is no data for that frequency...

            # todo: load from files and write to database.
            writer = self._create_writer(cal)

            # todo build lifetimes etc. we need to first ingest intraday
            #  data, with the sids then aggregate and retrieve the lifetimes...
            #  with the lifetimes and sids we can populate the equities table.

    def _create_writer(self, calendar):
        return mbw.ClickHouseMinuteBarWriter(
            self._db_config,
            'pluto_data',
            self._ingestion_state[calendar.name],
            calendar)

    def add_bar_writer(self, calendar):
        if calendar.name not in self._ingestion_state:
            self._ingestion_state[calendar.name] = {
                    'first_session': None,
                    'last_session': None,
                    'bars_written': False,
                    'lifetimes_written': False
                }

        self._writers[calendar.name] = mbw.ClickHouseMinuteBarWriter(
            self._db_config,
            'pluto_data',
            self._ingestion_state[calendar.name],
            calendar)

    def _ingest(self, root_dir, *args, **kwargs):
        # todo: ingest starting from last successful download...
        #  note: we only need to download data for failed assets...
        #  an asset is considered failed if it is not delisted and the datetime
        #  is before current datetime.
        #   we start from a list of symbols and we download all data.
        #   we need to populate: asset lifetimes (inferred from the historical data)
        #  keep track of update times for each asset?
        #  so we need some additional data about the state of our database.
        #  we could store the state in clickhouse database, that way we can easily query
        #  assets for which data have not been updated yet...
        #  for each successful download we update the data state...

        # todo: 1) retrieve tickers list
        #       2) download historical data of the tickers
        #       3) store data in a temporary file/folder
        #       Post-Processing:
        #       ----------------
        #       4) for each asset, compute lifetimes (store it in click house) and store last available dt
        #       5) store/update asset in asset finder.
        #       6)
        # we have a limit of 10000 elements per api call...

        # if we do not have data, download all assets...
        # else, download data.
        # todo: all active assets, download from last successful datetime...
        # todo: query assets where datetime < now
        # todo: we will use alpaca's asset id and map it to our sid system, such
        #  that we consistently update our database

        mode = kwargs['mode']
        client = self._alpaca_client
        session = kwargs['session']

        ingestion_state = self._ingestion_state

        initialized = ingestion_state['initialized']
        last_ingestion = ingestion_state['last_ingestion']

        # when we initialize, retrieve all the symbols
        if mode == 'initialize':
            symbols = [self._filter_symbols(
                client.list_assets('us_equity'),
                session)]

            # todo: download data from polygon.io

            timeframe = alp.TimeFrame(1, alp.TimeFrameUnit.Minute)

            if not initialized:
                # download everything starting from some start datetime
                last_ingestion = kwargs['start']
                # todo: max 200 symbols (split request into chunks of 200 symbols)
                # todo: specify start and end, since there is a limit
                # todo: returns certain amount elements per iteration per symbol
                # todo: we hit the rate limit if we request a time period that is too
                #  long
                # todo: we need to retrieve corporate actions (splits, dividends) from
                #  polygon.io ...

                # download one stock at a time...

            # downloads starting from last ingestion time...

            # todo: download from polygon.io
            #  problem: we only get 15 minute delayed data for the starter data...

            # download minute data, aggregate, compute asset lifetimes, update asset database

            # note: each minute we only download data for which we have positions...
            #  so we could switch to the alpaca free minute data
            # note: we only need data for a single minute for each ticker.
            #  but we only need to download minute data for assets for which we have positions.
            # at each end of session, we download data for all tickers, update the database etc.
            # note: with 15 minutes delayed data, we can wait 20 minutes, to get complete data...

            # so: we use alpaca realtime iex data for positions we have and use polygon.io for
            # historical data, with an offset of 20 minutes after session end of the calendar...
            # note: this should not block since other strategies may run on different calendars.

            # todo: we need to store the price data of the tracked assets in the database.
            # todo: we need quotes of the tracked positions...
            #  the historical prices (minutes etc.) are stored in the database...
            #  we need a way for the strategies to retrieve quotes...
            #  sync last sale prices uses this.

            # todo: could we use snapshots to get the most up-to-date quotes etc?
            # seems like its cheaper... (it's only for crypto in polygon.io)
            # we can get snapshots from alpaca... this would let update data at each iteration.

            client.get_bars_iter(
                symbols,
                timeframe,
                start=last_ingestion
            )

            # todo: make an initial write
            self._bar_writer.write()

            # todo: update our assets list

    def _filter_symbols(self, symbols, session):
        db_config = self._db_config

        with clickhouse_driver.Client(
                db_config['host'],
                db_config['port'],
                password=db_config['password'],
                database='alpaca') as cl:

            cl.execute('''
            SELECT * FROM assets WHERE last_update < %(dt)s
            ''', {'last_update': session})

            for symbol in symbols:
                if symbol.exchange not in self._exclude:
                    yield symbol

    def _exchange_data_generator(self, symbols):
        for symbol in symbols:
            exchanges = {}

            exchange = symbol['Exchange']

            if exchange not in exchanges:
                # yield {
                #     'exchange': exchange,
                #     'country_code': symbol['Country'],
                #     'currency': symbol['Currency'],
                #     'canonical_name': exchange
                # }

                yield exchange, exchange, symbol['Country'], symbol['Currency']

                exchanges[exchange] = exchange

    def _symbol_data_generator(self, symbols):
        for idx, symbol in enumerate(symbols):
            yield idx, symbol['Code'], symbol['Exchange'], symbol['Type']


class AlpacaBundleLoader(core.AbstractBundleLoader):
    def __init__(self, dir_interface):
        super(AlpacaBundleLoader, self).__init__()

        self._root_dir = dir_interface.get_directory('data').get_directory('alpaca')

    def _load(self):
        pass
