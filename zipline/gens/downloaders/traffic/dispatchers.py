from time import sleep
from collections import deque
from threading import Lock, Condition
from datetime import datetime


class Dispatcher:
	'''Receives and dispatches requests from and to different threads...'''

	def __init__(self):
		super(Dispatcher, self).__init__()
		self._queue = deque()
		self._lock = Lock()
		self._not_empty = Condition(self._lock)
		self._listeners = []

	def get_request(self, wait=False):
		with self._not_empty:
			if self._queue:
				request = self._queue.popleft()
				return request
			else:
				if wait:
					while not self._queue:
						self._not_empty.wait()
					item = self._queue.popleft()
				else:
					item = None
				self._not_empty.notify()
				return item

	def add_request(self, request):
		with self._lock:
			if isinstance(request, (list, tuple)):
				self._queue.extend(request)
			else:
				self._queue.append(request)
			self.notify()  # notify observers so that they can make requests...

	def register(self, executor):
		self._listeners.append(executor)

	def notify(self):
		for listener in self._listeners:
			listener.update(self)

	def __len__(self):
		return len(self._queue)


'''it's the clients job to give the right request to the right dispatcher... 
the client provide the request, the executor and ways of saving the result of the request...'''


class Schedule:
	def __init__(self, dispatcher, time_to_execution):
		if isinstance(time_to_execution, datetime):
			self._start_time = time_to_execution  # the datetime for execution... must be utc...
		else:
			raise TypeError("Expected : {0} got : {1}".format(datetime, type(time_to_execution)))
		self._dispatcher = dispatcher
		self._requests = []

	def add_request(self, request):
		self._requests.append(request)

	def __call__(self):
		t = self._start_time - datetime.utcnow()
		sleep_time = t.total_seconds()
		if sleep_time > 0:
			sleep(sleep_time)
		self._dispatcher.add_request(self._requests)
