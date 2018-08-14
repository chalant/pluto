from abc import ABC,abstractmethod

from zipline.pipeline.loaders.blaze import BlazeLoader,from_blaze
from zipline.pipeline.loaders import USEquityPricingLoader

from zipline.pipeline.data import USEquityPricing

class LoaderFactory(ABC):
	def __init__(self,bundle_data):
		self._bundle_data = bundle_data

	@abstractmethod
	def _create_loader(self, columns):
		raise NotImplementedError

	def __call__(self, column):
		if column in USEquityPricing.columns:
			bundle_data = self._bundle_data
			self._dataset = USEquityPricing
			return USEquityPricingLoader(bundle_data.equity_daily_bar_reader,
								   bundle_data.adjustment_reader)
		else:
			return self._create_loader(column)

class LoaderCreatorWrapper(LoaderFactory):
	def __init__(self,bundle_data,loader_creators):
		super(LoaderCreatorWrapper,self).__init__(bundle_data)
		self._loader_creators = loader_creators

	def _create_loader(self, column):
		creators = self._loader_creators
		for creator in creators:
			if column in creator:
				return creator.loader
		else:
			raise ValueError("No PipelineLoader registered for column {}.".format(column))

class LoaderCreator(ABC):
	@abstractmethod
	@property
	def dataset(self):
		raise NotImplementedError

	@abstractmethod
	@property
	def loader(self):
		raise NotImplementedError

class BlazeLoaderCreator(LoaderCreator):
	def __init__(self,fundamental_reader,columns):
		self._reader = fundamental_reader
		expr = self._reader.read(columns)
		self._loader = BlazeLoader()
		self._ds = from_blaze(expr,self._loader)

	@property
	def dataset(self):
		return self._ds

	@property
	def loader(self):
		return self._loader

	@property
	def columns(self):
		return self._reader.columns

	def __contains__(self, item):
		return item in self.dataset



