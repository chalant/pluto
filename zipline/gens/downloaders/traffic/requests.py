from abc import ABC,abstractmethod
from functools import partial


class Request(ABC):
	def __init__(self, saver):
		self._saver = saver
		self._save = None

	def save(self, data):
		if self._save:
			self._saver.save(partial(self._save,data=data))

	def __str__(self):
		return self._str()

	@abstractmethod
	def _str(self):
		raise NotImplementedError

	@property
	def saving_func(self):
		return self._save

	@saving_func.setter
	def saving_func(self, func):
		if not callable(func):
			raise AttributeError('func must be callable')
		self._save = func

class EquityRequest(Request):
	def __init__(self, saver):
		super(EquityRequest, self).__init__(saver)
		self._start_date = None
		self._end_date = None

	@property
	def symbol(self):
		return self._symbol

	@symbol.setter
	def symbol(self, value):
		self._symbol = value

	@property
	def frequency(self):
		return self._frequency

	@frequency.setter
	def frequency(self, value):
		self._frequency = value

	@property
	def start_date(self):
		return self._start_date

	@start_date.setter
	def start_date(self, value):
		self._start_date = value

	@property
	def end_date(self):
		return self._end_date

	@end_date.setter
	def end_date(self, value):
		self._end_date = value

	def _str(self):
		return "{0}".format(self._symbol)


class BatchEquityRequest(Request):
	'''downloads all most recent data of a list of symbols'''
	@property
	def symbols(self):
		return self._symbols

	@symbols.setter
	def symbols(self,values):
		self._symbols = values

class SandPConstituents(Request):
	def _str(self):
		return "s and p constituents"

class SandPHistory(Request):
	pass


def create_equity_request(saver,saver_func,symbol, start_date=None, end_date=None, frequency='1D'):
	request = EquityRequest(saver)
	request.symbol = symbol
	request.frequency = frequency
	request.start_date = start_date
	request.end_date = end_date
	request.saving_func = saver_func
	return request
