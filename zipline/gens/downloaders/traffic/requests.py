from abc import ABC


class Request(ABC):
	def __init__(self, saver):
		self._saver = saver

	def save(self, result):
		self._saver.save(result)


class EquityRequest(Request):
	def __init__(self, saver=None):
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


def create_equity_request(saver, symbol, start_date=None, end_date=None, frequency='1D'):
	request = EquityRequest(saver)
	request.symbol = symbol
	request.frequency = frequency
	request.start_date = start_date
	request.end_date = end_date
	return request
