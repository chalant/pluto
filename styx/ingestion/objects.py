import abc
import pickle

class Data(abc.ABC):
	@abc.abstractmethod
	def set_error(self, error):
		pass
	
	@abc.abstractmethod
	def __getstate__(self):
		pass

	@abc.abstractmethod
	def __setstate__(self, state):
		pass

	@abc.abstractmethod
	def set_error(self, task_name, status_code):
		pass

class Error(object):
	__slots__ = ('task', 'error')
	def __init__(self) -> None:
		self.task = None
		self.status_code = None
	
	def __getstate__(self):
		return {'task': self.task, 'status_code': self.status_code}
	
	def __setstate__(self, state):
		for slot in self.__slots__:
			setattr(self, slot, state[slot])

class EquityData(Data):
	__slots__ = ('bars', 'sid', 'dts', '_error')
	def __init__(self, sid, dts) -> None:
		self.bars = None
		self.sid = sid
		self.dts = dts
		self._error = Error()

	def set_error(self, task_name, status_code):
		self._error.task = task_name
		self._error.status_code = status_code
	
	def __getstate__(self):
		return {'bars': pickle.dumps(self.bars), 'sid': self.sid, 'dts': self.dts}
	
	def __setstate__(self, state):
		for slot in self.__slots__:
			setattr(self, slot, state[slot])

class AssetData(Data):
	def __init__(self, sid, symbol) -> None:
		super().__init__()

class CorporateAction(Data):
	pass