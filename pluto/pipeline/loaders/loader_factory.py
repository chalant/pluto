from zipline.pipeline.loaders.blaze import BlazeLoader, from_blaze
from zipline.pipeline.loaders import USEquityPricingLoader
from zipline.pipeline.data import USEquityPricing

from collections import Iterable


class PipelineDataLoaderFactory(object):
	'''class for creating loaders'''
	def __init__(self, bundle_data):
		self._bundle_data = bundle_data
		self._ds_and_loaders = {}

	def __call__(self, column):
		if column in USEquityPricing.columns:
			bundle_data = self._bundle_data
			return USEquityPricingLoader(bundle_data.equity_daily_bar_reader,
										 bundle_data.adjustment_reader)
		else:
			if column in self._ds_and_loaders:
				return self._ds_and_loaders[column][1]
			else:
				raise ValueError(
					"No PipelineLoader registered for column {}.".format(column)
				)

	def __getitem__(self, item):
		if not isinstance(item,Iterable):
			item = [item]
		return self._create_data_set(item)

	def _create_data_set(self, columns_names=None):
		'''Creates a data set and loader object and returns a data set (or bound column)
		object. If columns_names is a subset of previously created '''
		if columns_names is not None:
			if self._ds_and_loaders:
				if set(columns_names) < set(self._ds_and_loaders):
					ds, loader = self._ds_and_loaders[columns_names[0]]
					return tuple([getattr(ds, col) for col in columns_names])
				else:
					return self._create_loader(columns_names)
			else:
				return self._create_loader(columns_names)
		else:
			return self._create_loader(columns_names)

	def _create_loader(self, columns_names):
		bundle_data = self._bundle_data
		loader = BlazeLoader()
		data_set = from_blaze(self._load_blaze_expr(bundle_data.fundamentals_reader, columns_names),
							  bundle_data.fundamentals_reader, loader=loader)
		# add the data set and loader
		pair = (data_set, loader)
		for name in data_set:
			self._ds_and_loaders[name] = pair
		return tuple([getattr(col) for col in data_set])

	def _load_blaze_expr(self, fundamentals_reader, columns_names=None):
		return fundamentals_reader.read(columns_names)
