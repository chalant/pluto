import json

import clickhouse_driver
from alpaca_trade_api import rest
import alpaca_trade_api as alp

from pluto.data.bundles import core
from pluto.data.equity_pricing import bar_writer

from pluto.configuration import configuration


class AlpacaBundleIngester(core.AbstractBundleIngester):
    def __init__(self, root_dir, data_frequency='minute', exclude=('OTC',)):
        super(AlpacaBundleIngester, self).__init__('alpaca', root_dir)

        db_config = configuration['clickhouse']
        credentials = configuration['alpaca']

        root = root_dir.get_directory('data').get_directory('alpaca')
        state_file = root.add_file('state.json')

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

        self._rest_client = rest.REST(
            credentials['key_id'],
            credentials['secret_key'])

        with clickhouse_driver.Client(
                db_config['host'],
                db_config['port'],
                password=db_config['password']) as cl:

            cl.execute('''CREATE DATABASE IF NOT EXISTS alpaca''')

        with clickhouse_driver.Client(
                db_config['host'],
                db_config['port'],
                password=db_config['password'],
                database='alpaca'
        ) as cl:

            cl.execute('''CREATE TABLE IF NOT EXISTS lifetimes''')
            cl.execute('''CREATE TABLE IF NOT EXISTS asset_list''')

        self._db_config = db_config
        self._data_frequency = data_frequency

        if data_frequency == 'minute':
            bars = bar_writer.MinuteBars()
        else:
            bars = bar_writer.DailyBars()

        self._bar_writer = bar_writer.ClickHouseDataWriter(db_config, 'alpaca', bars)

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
        client = self._rest_client
        session = kwargs['session']

        ingestion_state = self._ingestion_state

        initialized = ingestion_state['initialized']
        last_ingestion = ingestion_state['last_ingestion']

        data_frequency = self._data_frequency

        # when we initialize, retrieve all the symbols
        if mode == 'initialize':
            symbols = [self._filter_symbols(
                client.list_assets('us_equity'),
                session)]

            if data_frequency == 'minute':
                timeframe = alp.TimeFrame(1, alp.TimeFrameUnit.Minute)
            else:
                timeframe = alp.TimeFrame(1, alp.TimeFrameUnit.Day)

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


class AlpacaBundleLoader(core.AbstractBundleLoader):
    def __init__(self, dir_interface):
        super(AlpacaBundleLoader, self).__init__()

        self._root_dir = dir_interface.get_directory('data').get_directory('alpaca')

    def _load(self):
        pass
