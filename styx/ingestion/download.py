import math
import json
import asyncio
import typing
import csv
import time

from functools import partial

from os import path
from io import StringIO

import clickhouse_driver

import pandas as pd
from pandas import Timestamp, Timedelta

import requests.exceptions

from sqlalchemy import engine

from alpaca_trade_api import rest
import alpaca_trade_api as alp

from trading_calendars import trading_calendar

from styx.ingestion import polygon_client

from pluto.data.equity_pricing import minute_bars_writer as mbw
from pluto.trading_calendars import calendar_utils
from pluto.configuration import configuration

# todo: this class should also generate pipeline modules in the
#  .pluto directory for pipeline we will then append modules in the sys.path


class EquityAsset(object):
    __slots__ = ('symbol', 'first_ts', 'last_ts')

    def __init__(self, symbol, first_ts, last_ts):
        self.symbol = symbol
        self.first_ts = first_ts
        self.last_ts = last_ts


def split(arr, size) -> typing.Generator:
    for i in range(0, len(arr), size):
        yield arr[i:i+size]


class DownloadHistoricalBars(object):
    def __init__(self, interval):
        self._interval = interval

    async def __call__(self,
                       index,
                       data_client,
                       error_handler,
                       symbols,
                       sessions,
                       root_dir_path,
                       progress,
                       buffer_size=3600):
        await download_historical_bars(
            index,
            data_client,
            self._interval,
            error_handler,
            symbols, sessions,
            root_dir_path,
            progress,
            buffer_size)


async def download_historical_bars(
        index,
        data_client,
        interval,
        error_handler,
        symbols,
        sessions: pd.DatetimeIndex,
        root_dir_path,
        progress,
        buffer_size=3600):

    if interval == 'm':
        down_fn = data_client.get_minute_historical_bars
    elif interval == 'd':
        down_fn = data_client.get_historical_bars
    else:
        error_handler.set_error(index, ValueError(f"interval {interval} not supported"))
        return

    for symbol in symbols:
        with open(path.join(root_dir_path, symbol + '.txt'), 'a') as f:
            buffer = []
            size = 0

            # download from the last ingested timestamp

            st, nd = sessions.slice_locs(
                pd.Timestamp.fromtimestamp(symbol.last_ts),
                sessions[-1])

            n_sessions = sessions[st: nd]

            for sess in split(n_sessions, math.ceil(len(n_sessions)/data_client.bars_limit)):
                try:
                    # todo: for 1m data, we get a timestamp, that we need to convert
                    data = down_fn(
                        index,
                        symbol,
                        sess[0], sess[-1])
                except requests.exceptions.RequestException as e:
                    # error could be payment error, etc.
                    error_handler.set_error(index, e)

                    if error_handler.is_fatal(index):
                        break

                buffer.append(data)

                elapsed = time.time() - t0

                # maybe wait
                t = data_client.should_wait(elapsed)

                if t:
                    await asyncio.wait(t)

                    t0 = time.time()
                    data_client.reset_calls(index)

                size += 1

                # todo: we could use double buffering to make this
                #   faster...

                # write buffered data...
                if size >= buffer_size:
                    # todo: write lifetimes (first datapoint and last datapoint)

                    # write data in file
                    for d in csv.reader(StringIO()):
                        f.write(str(d) + "\n")

                    # timestamp is the first element
                    symbol.first_ts = buffer[0][0]
                    symbol.last_ts = buffer[-1][0]

                    buffer.clear()
                    size = 0

        if error_handler.is_fatal(index):
            progress.terminate()
            break
        else:
            progress.increment()


async def download_splits(index, data_client, symbols, sessions, dir_path, progress):
    for symbol in symbols:
        data = data_client.get_splits(
            index,
            symbol,
            sessions[0],
            sessions[-1])

        # write data in file
        for d in data.json():
            f.write(str(d) + "\n")

        progress.increment()


async def download_dividends(index, data_client, symbols, sessions, dir_path, progress):
    for symbol in symbols:
        data = data_client.get_dividends(
            index,
            symbol,
            sessions[0],
            sessions[-1])

        # write data in file
        for d in data.json():
            f.write(str(d) + "\n")

        progress.increment()


def get_equity_assets(connection, sessions, table_name, calendar_name):
    # returns for which we have no data yet.
    res = []

    query = f'''
        SELECT symbol, first_timestamp, last_timestamp 
        FROM equity_assets
        JOIN {table_name}
        ON {table_name}.sid = equity_assets.sid
        JOIN calendars
        ON {table_name}.exchange = calendars.exchange
        WHERE calendar_name=?'''

    for res in connection.execute(query, (calendar_name,)):

        symbol, first_ts, last_ts = res

        # means that symbol has never been ingested...
        if last_ts == -1:
            last_ts = sessions[0]
            first_ts = sessions[0]

        # only download if the data is not up-to-date
        if last_ts < sessions[-1]:
            res.append(EquityAsset(
                symbol,
                first_ts,
                last_ts))

    return res


class DataIngestion(object):
    def __init__(self, root_dir):

        db_config = configuration['clickhouse']
        alpaca_credentials = configuration['alpaca']
        self._polygon_token = configuration['polygon.io']['api_token']

        self._root_dir = root = root_dir.get_directory('data').get_directory('styx')
        state_file = root.add_file('state.json')

        ingestion = root.add_file('ingestion.sqlite')
        self._db_engine = engine.create_engine('sqlite://{}'.format(ingestion.path))

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

    async def _download(self, progress, sessions, num_threads, response_handler):

        _operating_mic_to_calendar = {
            'XNAS': 'XNYS',
            'XNYS': 'XNYS'
        }

        def _filter_exchanges(exchanges_):
            # filters exchanges for which we have calendars...
            for exchange in exchanges_:
                mic = exchange['operating_mic']
                if mic in _operating_mic_to_calendar:
                    exchange['calendar'] = _operating_mic_to_calendar[mic]

            return exchanges_

        def _get_operating_mic(exchanges_):
            for exchange in exchanges_:
                yield exchange['operating_mic'], exchanges_['calendar'], exchange['locale'], exchange['asset_class']

        def _exchange_data_generator(exchanges_):
            for exchange in exchanges_:
                yield exchange['name'], exchange['mic'], exchange['operating_mic']

        def _get_asset_types(client_):
            for asset_type in client_.get_asset_types():
                yield asset_type['code'], asset_type['asset_class'], asset_type['description']

        def _symbol_data_generator(client_, market):
            for idx, symbol in enumerate(client_.get_symbols(market)):
                yield idx, symbol['ticker'], symbol['name'], symbol['type'], symbol['primary_exchange']

        with polygon_client.PolygonClient(self._polygon_token, num_threads, response_handler, 50000) as client:
            with self._db_engine.connect() as connection:
                connection.execute(
                    '''
                    INSERT INTO countries (country_code, currency)
                    VALUES (?, ?)
                    ''', ('us', 'usd'))
                connection.execute(
                    '''
                    INSERT INTO markets (market)
                    VALUES (?)
                    ''',('stock',))
                exchanges = _filter_exchanges(client.get_exchanges('stocks'))
                connection.execute(
                    '''
                    INSERT INTO calendars (calendar_name)
                    (?)
                    ''',
                    ('XNYS',))
                connection.execute(
                    '''
                    INSERT INTO operating_mics (operating_mic, calendar, country_code, market)
                    VALUES (?, ?, ?, ?)
                    ''',
                    _get_operating_mic(exchanges))
                # store exchanges
                connection.execute(
                    '''
                    INSERT INTO exchanges (exchange_name, mic, operating_mic) 
                    VALUES (?, ?, ?, ?)''',
                    _exchange_data_generator(exchanges))
                connection.execute(
                    '''
                    INSERT INTO asset_types (asset_type, market, description)
                    ''',
                    _get_asset_types(client))
                # insert assets
                connection.execute(
                    '''
                    INSERT INTO equity_assets (sid, symbol, asset_type, first_timestamp, last_timestamp, exchange)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ''',
                    _symbol_data_generator(client, 'stocks'))

            bars_progress = progress.fork()
            bars_progress.task_name = "Minute Bars"
            splits_progress = progress.fork()
            splits_progress.task_name = "Splits"
            dividends_progress = progress.fork()
            dividends_progress.task_name = "Dividends"

            tasks = (
                asyncio.create_task(
                    self._download_data(
                        client,
                        DownloadHistoricalBars('m'),
                        "bars",
                        bars_progress,
                        sessions,
                        "",
                        num_threads)),
                asyncio.create_task(
                    self._download_data(
                        client,
                        download_splits,
                        "splits",
                        splits_progress,
                        sessions,
                        "",
                        num_threads)),
                asyncio.create_task(
                    self._download_data(
                        client,
                        download_dividends,
                        "dividends",
                        dividends_progress,
                        sessions,
                        "",
                        num_threads)))

            await asyncio.gather(*tasks)

    def ingest(self, progress, num_threads=10):
        # we are using polygon for all the data for now...
        # for some parts we might use eodhd or other sources...

        ingestion_state = self._ingestion_state
        initialized = ingestion_state['initialized']
        last_ingestion = ingestion_state['last_ingestion']

        if not initialized:
            # initial write.

            # todo: once everything is ingested, further manual mapping
            #  in the table must be performed, since we don't always know which
            #  exchanges map to which calendars...

            # todo: memory map symbols?

            # todo: we need to map the exchanges to our internal calendars
            # todo: this could be done through some cli-tool and a file such as json or toml for mappings...

            # split into k groups (say 10)
            # create writer for each group
            writer = self._writers['XNYS']

            # todo: pre-store sid to symbol mappings
            #  note: since the symbol changes, it is not a primary key
            #  we will be holding a history of symbols mapped to the sid
            #  in order to track any symbol changes and map it back to that sid.

            response_handler = None

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

            end = Timestamp.utcnow()
            start = end - Timedelta(years=2)
            cal = calendar_utils.get_calendar_in_range('NYSE', start, end)
            sessions = cal.sessions_in_range(start, end)

            # todo: fork should automatically increase target
            progress.task_name = "Ingesting Data"

            download_progress = progress.fork()
            download_progress.task_name = "Downloading"

            # download data... this blocks...

            asyncio.run(self._download(
                download_progress,
                sessions,
                num_threads,
                response_handler))

            # todo: ingest data here...
            #  this should create lifetimes, aggregate daily bars etc.
            #  and also ingest asset data...
            #  we do this in a single thread, no point using multiple threads
            # todo: we need to equity data of symbol changes.

            # todo: store data frequency mappings with each symbol...
            # todo: since we don't know for which ticker we have data for a certain frequency,
            #  if a request returns nothing then there is no data for that frequency...

            # todo: load from files and write to database.
            writer = self._create_writer(cal)

            # todo build lifetimes etc. we need to first ingest intraday
            #  data, with the sids then aggregate and retrieve the lifetimes...
            #  with the lifetimes and sids we can populate the equities table.
            ingestion_progress = progress.fork()

    # def _create_writer(self, calendar):
    #     return mbw.ClickHouseMinuteBarWriter(
    #         self._db_config,
    #         'pluto_data',
    #         self._ingestion_state[calendar.name],
    #         calendar)

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

    async def _download_data(self, client, target_fn, table_name, task_progress, sessions, dir_path, num_threads):
        tasks = []

        with self._db_engine.connect() as connection:
            target_assets = get_equity_assets(connection, sessions, table_name, 'XNYS')

        num = len(target_assets)
        task_progress.target = num

        for i, batch in enumerate(split(target_assets, math.ceil(num / num_threads))):
            sub_progress = task_progress.fork()

            sub_progress.task_name = f"Thread {i}"
            sub_progress.target = len(batch)

            tasks.append(asyncio.create_task(target_fn(i, client, batch, sessions, dir_path, sub_progress)))

        await asyncio.gather(*tasks)

        with self._db_engine.connect() as connection:
            for equity_asset in target_assets:
                connection.execute(
                    '''
                    UPDATE ? 
                    SET first_timestamp=?, second_timestamp=?
                    WHERE symbol=?)''',
                    (table_name, equity_asset.first_ts, equity_asset.last_ts, equity_asset.symbol)
                )

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

            # todo: download data from polygon.io

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

            # todo: make an initial write
            self._bar_writer.write()

            # todo: update our assets list

