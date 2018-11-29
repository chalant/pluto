from abc import ABC, abstractmethod

from trading_calendars.calendar_utils import global_calendar_dispatcher as gcd


class Ingestible(ABC):
	@abstractmethod
	def get_ingester(self,
					 metadata,
					 environ,
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
		raise NotImplementedError


class Ingester(ABC):
	def __init__(self, metadata):
		self._metadata = metadata

	@abstractmethod
	def ingest(self, show_progress=False):
		raise NotImplementedError


class MetaDataIngester(Ingester):
	def __init__(self, metadata, asset_db_writer, calendar, start_session, end_session, metadata_collection):
		super(MetaDataIngester, self).__init__(metadata)
		self._writer = asset_db_writer
		self._mc = metadata_collection
		self._cal = calendar
		self._st_sess = start_session
		self._end_sess = end_session

	def ingest(self, show_progress=False):
		'''todo : ingest metadata (with the asset_db_writer)'''
		pass


class MinuteEquityIngester(Ingester):
	def __init__(self, metadata, minute_bar_writer, calendar, start_session, end_session, minute_equity_collection):
		super(MinuteEquityIngester, self).__init__(metadata)
		self._writer = minute_bar_writer
		self._collection = minute_equity_collection
		self._calendar = calendar
		self._start_session = start_session
		self._end_session = end_session

	def _create_generator(self, bar_collection, calendar, start_session, end_session):
		for sid, symbol, exchange in self._md.by_exchange():
			if gcd.resolve_alias(exchange) == calendar.name:
				# returns a pandas dataframe containing the bars
				yield sid, bar_collection.get_data(symbol, start_session, end_session)

	def _write(self, generator, show_progress):
		self._writer.write(generator, show_progress=show_progress)

	def ingest(self, show_progress=False):
		self._write(self._create_generator(self._collection, self._calendar, self._start_session, self._end_session),
					show_progress)


class DailyEquityIngester(Ingester):
	def __init__(self, metadata, daily_bar_writer, calendar, start_session, end_session, daily_equity_collection):
		super(DailyEquityIngester, self).__init__(metadata)
		self._dec = daily_equity_collection
		self._writer = daily_bar_writer
		self._calendar = calendar
		self._start_session = start_session
		self._end_session = end_session

	def _create_generator(self, bar_collection, calendar, start_session, end_session):
		for sid, symbol, exchange in self._md.by_exchange():
			if gcd.resolve_alias(exchange) == calendar.name:
				# returns a pandas dataframe containing the bars
				yield sid, bar_collection.get(symbol, start_session, end_session)

	def _write(self, generator, show_progress):
		self._writer.write(generator, show_progress = show_progress)

	def ingest(self, show_progress=False):
		self._write(self._create_generator(self._dec, self._calendar, self._start_session, self._end_session),
					show_progress)
