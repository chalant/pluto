import asyncio
import pandas as pd

from styx.ingestion import caches
from styx.ingestion import objects

async def update_symbol_list(ts: pd.Timestamp, downloader, calendar, assets_db) -> None:
	# we are going to need multiple data sources (alpaca, polygon.io, open-figi.)
	# note: we assume that assets with the same exchange and same symbol are equal because no two assets on the same exchange can share the same symbol
	
	asset_state = 'active'
	# todo: we could say that two assets are equal if they share the same symbol, exchange, country code, currency etc.
	asset_list = set(await downloader.asset_list(ts, state=asset_state))
	stored_assets = set(assets_db.get_assets(state=asset_state))    
	# seems like we can query for assets at a specific date in polygon.io
	# if an asset does not appear, it has probably changed names    
	new_assets = asset_list.difference(stored_assets)
	start = calendar.previous_close(ts)
	
	if new_assets:
		#we are going to use alpaca for historical data and polygon io for realtime...
		
		with caches.MMapCache() as corporate_actions:
			await downloader.corporate_actions(start, ts, new_assets, corporate_actions)
			# todo: for assets for which we do not have corporate actions, we can make an open figi query.
			assets_db.set_corporate_actions(corporate_actions)
			# re-load assets after applying corporate actions.
		
		with caches.MMapCache() as symbol_changes:
			changed_names = set(assets_db.get_assets(state=asset_state)).difference(asset_list)
			await downloader.name_changes(start, ts, changed_names, symbol_changes)
			# todo: this should also merge data with the previous symbol.
			assets_db.set_symbol_changes(symbol_changes)
		
		# reload after applying name changes and corporate actions.
		stored_assets = assets_db.get_assets(state=asset_state)
		assets_db.set_as_delisted(stored_assets).difference(asset_list)
		# add new assets.
		assets_db.add_assets(asset_list.difference(stored_assets))
	
	# todo: we can also detect changes when price is missing for an asset in alpaca. (null price or something...)
	# we should write them in some file to be processed later... (shoul be written in the asset db)
	# assets_db.

async def _download_mb_asset(downloader, data, asset, cache_pool):
	cache = await cache_pool.get_cache()
	downloader.minute_bars(data, asset, cache)
	cache_pool.put_cache(cache)

async def ingest_minute_bars(start_ts: pd.Timestamp, end_ts:pd.Timestamp, downloader, assets, calendar, minute_bar_writer) -> None:
	tasks = []
	with caches.MmapCachePool() as pool:
		dts = calendar.minutes_in_range(start_ts, end_ts)
		for asset in assets:
			tasks.append(asyncio.create_task(_download_mb_asset(downloader, objects.EquityData(asset.sid, dts), asset, pool)))
		await asyncio.gather(*tasks)
		for data in pool:
			# todo: check if there is an error (429 -> too many requests, 403 -> forbidden etc. 403 should terminate, 429 should retry after some time...)
			minute_bar_writer.write_cols(data.sid, data.dts, data.bars)

async def ingest_splits(start_ts: pd.Timestamp, end_ts:pd.Timestamp, downloader, assets, minute_bar_writer) -> None:
	 pass

async def ingest_dividends(start_ts: pd.Timestamp, end_ts:pd.Timestamp, downloader, assets, minute_bar_writer) -> None:
	 pass

async def update_daily_data(minute_bar_writer, calendar) -> None:
	   # todo: this will aggregate daily bars and produce asset lifetimes data for the specified calendar.
	   pass
