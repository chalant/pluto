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
		self._failed = {}
		self._failed_tuple_queue = None

	def _prepare_failed(self, name):
		if self._failed:
			if self._failed_tuple_queue:
				return self._failed_tuple_queue.popleft()
			else:
				self._failed_tuple_queue = self._failed.popitem()[1]
				return self._prepare_failed(name)
		else:
			return None

	def _prepare_request(self,wait):
		if self._queue:
			return self._queue.popleft()
		else:
			if wait:
				while not self._queue:
					self._not_empty.wait()
				item = self._queue.popleft()
			else:
				item = None
			self._not_empty.notify()
			return item


	def get_request(self, name,wait=False):
		with self._not_empty:
			if self._failed and name not in self._failed: #execute the failed requests first...
				#if the executor isn't in the
				request = self._prepare_failed(name)
				if request:
					return request
				else:
					return self._prepare_request(wait)
			else:
				return self._prepare_request(wait)

	def add_request(self, request):
		with self._lock:
			if isinstance(request, (list, tuple)):
				self._queue.extend(request)
			else:
				self._queue.append(request)
			self.notify()  # notify observers so that they can make requests...

	def failed(self,name,request):
		with self._not_empty:
			if name not in self._failed:
				arr = deque()
				self._failed[name] = arr
			else:
				arr = self._failed[name]
			arr.append(request)

	def register(self, executor):
		self._listeners.append(executor)

	def notify(self):
		for listener in self._listeners:
			listener.update(self)

	def __len__(self):
		return len(self._queue)

	def __iter__(self):
		return iter(self._listeners)


'''it's the clients job to give the right request to the right dispatcher... 
the client provide the request, the executor and ways of saving the result of the request...'''


class Schedule:
	def __init__(self, dispatcher, time_to_execution,thread_pool): #wraps around a dispatcher
		self._time = time_to_execution
		self._dispatcher = dispatcher
		self._requests = []
		self._pool = thread_pool
		print('scheduled for execution in: ',time_to_execution,' seconds')

	def add_request(self, request):
		self._requests.append(request)

	def __call__(self):
		print('sleeping...')
		sleep(self._time)
		self._dispatcher.add_request(self._requests)
		for executor in self._dispatcher: #submit executors...
			self._pool.submit(executor)

	def __len__(self):
		return len(self._requests)
