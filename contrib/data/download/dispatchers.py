from collections import deque
from threading import Lock, Condition

class Dispatcher:
	'''Receives and stores requests and executors call it to fetch requests'''
	def __init__(self):
		self._queue = deque()
		self._lock = Lock()
		self._not_empty = Condition(self._lock)
		self._listeners = []
		self._failed = {}
		self._failed_tuple_queue = None

	def _prepare_failed(self):
		if self._failed_tuple_queue:
			return self._failed_tuple_queue.popleft()
		else:
			failed_tuple_queue = self._failed.popitem()[1]
			self._failed_tuple_queue = failed_tuple_queue
			return failed_tuple_queue.popleft()

	def _prepare_request(self, wait):
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

	def get_request(self, name, wait=False):
		with self._not_empty:
			#if we have a failed request give to the next executor
			if self._failed and name not in self._failed:
				return self._prepare_failed()
			else:
				return self._prepare_request(wait)

	def add_request(self, request):
		with self._lock:
			if isinstance(request, (list, tuple)):
				self._queue.extend(request)
			else:
				self._queue.append(request)
			self.notify()  # notify observers so that they can make requests...

	def failed(self, name, request):
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
