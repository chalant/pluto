import typing
import asyncio

from collections import defaultdict

import pandas as pd

from protos.clock_pb2 import (
	SESSION_START,
	BAR,
	TRADE_END,
	BEFORE_TRADING_START,
	SESSION_END)

from pluto.control.controller import behavior
from pluto.control.controller import process_manager as pm

from styx.ingestion import tasks


class TaskExecutor(object):
	def __init__(self):
		self._tasks = []

	def add_task(self, coroutine):
		self._tasks.append(asyncio.create_task(coroutine))

	async def _execute(self):
		await asyncio.gather(*self._tasks)

	def execute(self):
		if self._tasks:
			asyncio.run(self._execute())
			self._tasks.clear()


# todo: we need to specify where to ingest data in the downloader...
#  we could specify where to write the data... -> we need to specify a writer
#  which writes to a specific database or table...
#  or we could just get read from the downloader in a function and write where we
#  want, by the downloader must return data in a certain format and we
#  need a function for each task that knows how to read data for a particular endpoint


class LiveControllerBehavior(behavior.AbstractControllerBehavior):
	def __init__(self, downloader, ingestion_db, ingestion):

		self._all_assets = set()
		self._downloader = downloader
		self._ingestion_db = ingestion_db
		self._ingestion = ingestion
		self._task_executor = TaskExecutor()
		self._writers = {}
		self._readers = {}
		#for writing in the live database...
		self._live_writers = {}
		self._start_ts = None
		self._asset_status = 'active'

		self._calendar_classifier = defaultdict(list)

	def get_last_sale_prices(self, ts, manager, signals, handle_non_market_minutes=False):
		for cts, evt, calendar in signals:
			if evt == BAR or evt == TRADE_END or evt == BEFORE_TRADING_START:
				pass

	def update_data(self, ts: pd.Timestamp, manager: pm.StrategyManager, signals: typing.List):
		# todo: we need to execute BAR and TRADE_END event first then return
		self._update_symbol_list(ts, signals)
		self._ingest_watchlist_bar_data(ts, manager, signals)
		self._ingest_bar_data(ts, manager, signals)
	
	def ingest_watchlist_data(self, ts: pd.Timestamp, manager: pm.StrategyManager, signals: typing.List):
		self._update_symbol_list(ts, signals)
		self._ingest_watchlist_bar_data(ts, manager, signals)

	def ingest_all_data(self, ts: pd.Timestamp, manager: pm.StrategyManager, signals: typing.List):
		self._ingest_bar_data(ts, manager, signals)

	def update_watchlist(self, ts, manager: pm.StrategyManager) -> None:
		watchlist = manager.get_watchlist()
		new_watchlist = set(watchlist.values())
		all_assets = self._all_assets
		executor = self._task_executor
		# assets that are not in the live database that we need to transfer
		# from the main database (these assets were never in the database, so ingesting them is safe)
		to_ingest = new_watchlist.difference(all_assets)
		readers = self._readers
		writers = self._writers
		per_calendar = self._calendar_classifier
		per_calendar.clear()
		if to_ingest:
			for asset in to_ingest:
				per_calendar[asset.calendar_name].append(asset)
			# copy data for assets to ingest from main database to live database
			for calendar_name, assets in per_calendar.items():
				reader = readers[calendar_name]
				writer = writers[calendar_name]
				executor.add_task(self._transfer_bars(ts, assets, reader, writer))
				executor.add_task(self._transfer_splits(ts, assets, reader, writer))
				executor.add_task(self._transfer_dividends(ts, assets, reader, writer))

		executor.execute()
		# keep track of already ingested assets
		self._all_assets = all_assets | to_ingest

	def _update_symbol_list(self, ts: pd.Timestamp, signals: typing.List) -> None:
		ingestion_db = self._ingestion_db
		executor = self._task_executor
		downloader = self._downloader

		# check for symbol changes before trading starts...
		for _, evt, calendar in signals:
			if evt == BEFORE_TRADING_START:
				executor.add_task(tasks.update_symbol_list(
					ts,
					downloader,
					calendar,
					ingestion_db.get_asset_db(calendar.name)))
		executor.execute()
		# todo: the symbol changes task also merges data with the previous symbols data...
	def _ingest_watchlist_bar_data(self, ts: pd.Timestamp, manager: pm.StrategyManager, signals: typing.List) -> None:
		# ingest data for positions and watchlist
		executor = self._task_executor
		downloader = self._downloader
		watchlist = manager.get_watchlist()
		writers = self._live_writers
		to_download = []
		pm = None
		# retrieve assets for which we have positions
		for _, c_evt, calendar in signals:
			if c_evt == BAR or c_evt == TRADE_END:
				for position in manager.get_positions(calendar.name):
					to_download.append(position.asset)
				if calendar.name in watchlist:
					to_download.extend(watchlist[calendar.name])
				pm = calendar.previous_minute(ts)
				executor.add_task(tasks.ingest_minute_bars(
					pm, ts, downloader, set(to_download), writers[calendar.name]))
			to_download.clear()
		executor.execute()

	def _ingest_bar_data(self, ts: pd.Timestamp, manager, signals: typing.List) -> None:
		executor = self._task_executor
		downloader = self._downloader
		writers = self._writers
		ingestion_db = self._ingestion_db
		status = self._asset_status
		# note: each task also ingests data
		for _, evt, calendar in signals:
			if evt == SESSION_END:
				writer = writers[calendar.name]
				assets_gen = ingestion_db.get_assets(calendar.name, status)
				st = calendar.previous_minute(ts);
				executor.add_task(tasks.ingest_minute_bars(
					st, ts, downloader, assets_gen, writer))
				executor.add_task(tasks.ingest_splits(
					st, ts, downloader, assets_gen, writer))
				executor.add_task(tasks.ingest_dividends(
					st, ts, downloader, assets_gen, writer))

		executor.execute()
		# creates lifetime index etc.
		for _, evt, calendar in signals:
			executor.add_task(tasks.update_daily_data(writers[calendar.name], calendar))
		executor.execute()
		watchlist = manager.get_watchlist()
		if watchlist:
			readers = self._readers
			lw = self._live_writers
			for calendar_name, assets in watchlist.items():
				reader = readers[calendar_name]
				writer = lw[calendar_name]
				# todo: clear live database then transfer data from 
				# note: the transfer function will clear the live table and copy data.
				executor.add_task(self._transfer_bars(ts, assets, reader, writer))
				executor.add_task(self._transfer_splits(ts, assets, reader, writer))
				executor.add_task(self._transfer_dividends(ts, assets, reader, writer))
				# todo:
				#  once the ingestion is done, drop live database and copy data from watched assets to new live database
				#  only ingest data for assets in the watchlist. Note: this have the effect
				#  of also cleaning up assets that are not in the watchlist...
				#  calling update watchlist will copy data for
				# todo: we have a table per calendar... if we have multiple calendars we need to merge data...
		executor.execute()
