from abc import ABC,abstractmethod

class MetaData(ABC):
	@abstractmethod
	def get_metadata(self,symbol):
		raise NotImplementedError

class DailyEquity(ABC):
	@abstractmethod
	def get_daily_equity(self,start_date,end_date):
		raise NotImplementedError

class MinutelyEquity(ABC):
	@abstractmethod
	def get_minutely_equity(self,request):
		raise NotImplementedError

class SPYConstituents(ABC):
	@abstractmethod
	def get_spy_constituents(self):
		raise NotImplementedError