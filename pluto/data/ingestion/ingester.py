class Ingester(object):
	def __init__(self,
				 daily_bar_writer,
				 minute_bar_writer,
				 adjustment_writer,
				 assets_metadata_writer):
		self._daily_bar = daily_bar_writer
		self._minute_bar = minute_bar_writer
		self._adjustment = adjustment_writer
		self._assets_metadata = assets_metadata_writer

	def ingest_daily_bars(self, country_code, frames):
		'''

        Parameters
        ----------
        country_code: str
        frames: iterable[tuple(str, pandas.DataFrame)]

        '''
		self._daily_bar.write(country_code, frames)

	def ingest_minute_bars(self, country_code, frames):
		'''

        Parameters
        ----------
        country_code: str
        frames: iterable[tuple(str, pandas.DataFrame)]

        '''
		self._minute_bar.write(country_code, frames)
		raise NotImplementedError('Minute bars not supported yet.')

	def ingest_adjustments(self, frames):
		#todo
		pass

	def ingest_assets_metadata(self,
							   equities,
							   futures,
							   exchanges,
							   root_symbols,
							   equity_supplementary_mappings):
		self._assets_metadata.write(
			equities,
			futures,
			exchanges,
			root_symbols,
			equity_supplementary_mappings)
		raise NotImplementedError(self.ingest_assets_metadata.__name__)