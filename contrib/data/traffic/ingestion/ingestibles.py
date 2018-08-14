from abc import ABC,abstractmethod

from zipline.data.bundles import dynamic

from contrib.data.traffic.storage.queryable import DailyBars

from pandas import DataFrame

#returns ingestion data : data in the proper format + info like timeframe,exchange,start_date,end_date,
#and the type of data...


class Ingestible(ABC):
	'''a metadata provides data about a particular symbol
	an ingester is an iterable that yields a sid,and a dataframe'''
	@abstractmethod
	def get_ingester(self,metadata):
		pass
'''an iterable that returns sid and dataframe pair...'''
class Ingester(ABC):
	def __init__(self,metadata):
		self._metadata = metadata

	def __iter__(self):
		return self

	def __next__(self):
		symbol,df = self._next()
		for metadata in self._metadata.equity_metadata:
			metadata.sid
		return self._metadata.get_sid(symbol),df

	@abstractmethod
	def ingest(self, environ,
			   asset_db_writer,
			   minute_bar_writer,
			   daily_bar_writer,
			   adjustment_writer,
			   fundamentals_writer,
			   calendar,
			   start_session,
			   end_session,
			   cache,
			   show_progress,
			   output_dir):
		pass

	@abstractmethod
	def _next(self):
		raise NotImplementedError

class MetaDataIngester(Ingester):
	def __init__(self):
		pass

	@abstractmethod
	def ingest(self, environ,
			   asset_db_writer,
			   minute_bar_writer,
			   daily_bar_writer,
			   adjustment_writer,
			   fundamentals_writer,
			   calendar,
			   start_session,
			   end_session,
			   cache,
			   show_progress,
			   output_dir):
		pass

class DailyEquityIngester(Ingester):
	def __init__(self,equity_meta_data,daily_bars:DailyBars):
		super(DailyEquityIngester,self).__init__(equity_meta_data)
		self._md = equity_meta_data
		self._db = daily_bars

	def ingest(self,environ,
			   asset_db_writer,
			   minute_bar_writer,
			   daily_bar_writer,
			   adjustment_writer,
			   fundamentals_writer,
			   calendar,
			   start_session,
			   end_session,
			   cache,
			   show_progress,
			   output_dir):
		#problem: how do we know what is the 'proper' calendar?, calendars are mapped
		#exchanges...
		def gen():
			for sid,symbol in self._md:
				yield sid,self._db.get_bars(symbol,calendar)
		daily_bar_writer.write(gen())

class AssetDataIngester(Ingester):
	def __init__(self,meta_data,equity_data_collection):
		super(AssetDataIngester,self).__init__(meta_data)





