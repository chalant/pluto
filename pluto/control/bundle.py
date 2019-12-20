from abc import ABC, abstractmethod

from functools import partial

from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import datetime as dt
import pytz

from zipline.data.bundles.core import register, ingest, load
from zipline.utils.run_algo import run_algorithm

class BundleCreator(object):
	'''handles bundle creation'''

	def __init__(self, downloaders, metadata):
		self._downloaders = downloaders
		self._metadata = metadata

	def _get_ingester(self,
					  downloader,
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
		return downloader.get_ingester(
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
			output_dir)

	def _create_ingest_func(self, metadata, ingestibles):
		def ingest_func(environ,
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
			for ingestible in ingestibles:
				self._ingest(self._get_ingester(
					ingestible,
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
					output_dir))
			return ingest_func

	def create_bundle(self, name, start_session, end_session):
		'''returns a bundle'''
		register(name, self._create_ingest_func(self._metadata, self._downloaders), start_session, end_session)
		return load(name)

	def _ingest(self, ingester, show_progress=False):
		ingester.ingest(show_progress)


class DownloadHandler(ABC):
	'''base class for implementing a download handler, download handler is provided in the
	session class for downloading anything useful for the strategy, the download handler also
	can return a data loader factory for loading ingested data into a pipeline loader'''

	def download(self, start_session, end_session_dt):
		# first, download data.
		downloadables = self._create_downloadables()
		downloaders = []
		metadata = self._create_metadata_downloadable()
		# first download metadata...
		with ThreadPoolExecutor() as pool:
			mt_downloader = self._get_downloader(metadata)
			self._download(mt_downloader, pool, end_session_dt)
			downloaders.append(mt_downloader)

		with ThreadPoolExecutor() as pool:
			for downloadable in downloadables:
				downloader = self._get_downloader(downloadable)
				self._download(downloader, pool, end_session_dt)
				downloaders.append(downloader)

		return BundleCreator(downloaders, metadata)

	def _get_downloader(self, downloadable):
		return downloadable.get_downloader()

	def _download(self, downloader, thread_pool, today):
		downloader.download(thread_pool, today)

	@abstractmethod
	def _create_downloadables(self):
		# must return a list or tuple of downloadables... ex:S&P500Downloader, DailyEquity...
		raise NotImplementedError

	@abstractmethod
	def _create_metadata_downloadable(self):
		raise NotImplementedError


class Strategy(ABC):
	def initialize(self, context, create_data_set):
		self._initialize(context, create_data_set)

	def create_data_set(self,columns):
		pass

	@abstractmethod
	def _initialize(self, context, data):
		raise NotImplementedError

	@abstractmethod
	def _handle_data(self, context, data):
		raise NotImplementedError

	def _before_trading_starts(self, context, data):
		raise NotImplementedError

	@abstractmethod
	def _save(self):
		'''sub-classes must implement this in order to save the strategy's state...'''
		raise NotImplementedError

	@abstractmethod
	def _load(self):
		raise NotImplementedError


class SessionState(object):
	pass


class Session(object):
	'''a session is a client class for the server... handles data ingestion and updates...'''
	# TODO: use finite state pattern to handle live trading => one session can have multiple
	def __init__(self, name, strategy):
		self._name = name
		self._strategy = strategy

	# todo: with metrics_set, we know how to 'decorate' the algorithm object to show animation...
	def run(self, start_session, end_session, capital_base, data_frequency='daily', metrics_set='default'):
		'''handles algorithm instanciation, etc.'''
		'''start session and end_session must be UTC'''
		# TODO: utc-fy the start session and end session if they are naïve or use some other
		# timezone...
		if start_session > end_session:
			raise ValueError('the start date must be smaller that the end date')
		elif end_session > pd.Timestamp.utcnow().date():
			raise ValueError('the end date must be equal or smaller than the current date')

		if isinstance(end_session, dt.date):
			end_sess_dt = dt.datetime.combine(end_session, dt.time.min, tzinfo=pytz.UTC)
		# first, download data on until the end_session (for backtesting...)
		# TODO: this routine is done else where => encapsulate
		name = self._name
		# create a bundle
		bundle = self._create_bundle(name, self._download(self._strategy.downloader), start_session, end_session)
		# ingest the data
		ingest(name, True)  # set show_progress to true?
		# create a data loader factory from the bundle...
		dlf = self._create_data_loader_factory(bundle)

		initialize = self._strategy.initialize
		handle_data = self._strategy.handle_data
		before_trading_starts = self._strategy.before_trading_starts

		run_algorithm(start_session, end_session, capital_base=capital_base,
					  initialize=partial(initialize, data_loader_factory=dlf),
					  handle_data=handle_data, before_trading_start=before_trading_starts,
					  pipeline_loader_factory=dlf, bundle=name)

	def _create_bundle(self, name, bundle_creator, start_session, end_session):
		'''returns a registered bundle'''
		return bundle_creator.create_bundle(name, start_session, end_session)

	def _download(self, downloader, start_session, end_session_dt):
		return downloader.download(start_session, end_session_dt)

	def _create_data_loader_factory(self, bundle):
		# TODO: create a data loader factory from a bundle
		return None

	def close(self):
		# closes the session...
		self._strategy.save()

	def open(self):
		self._strategy.load()

	def run_back_test(self):
		'''runs strategy back_test...'''
		pass
