from abc import ABC, abstractmethod
from time import sleep
from threading import Lock,Condition
from logbook import Logger

log = Logger('Executor')

class _RequestExecutor(ABC):
	def __init__(self, name):
		self._cool_down = self._cool_down_time()
		self._used = False
		self._dispatcher_set = set()
		self._current_dispatcher = None
		self._prb = None
		self._current_dispatcher = None
		self._name = name
		self._executing = Condition(Lock())

	def __call__(self):
		with self._executing:
			while True:
				if self._current_dispatcher is None:
					try:
						self._current_dispatcher = self._dispatcher_set.pop()
					except KeyError:
						break
				else:
					request = self._get_request(self._current_dispatcher)
					if request:
						self._check_used()
						self._save(request)
					else:
						self._current_dispatcher = None
			self._executing.notify()

	def _check_used(self):
		if not self._used:
			self._used = True
		else:
			sleep(self._cool_down)

	def _get_request(self, dispatcher):
		return dispatcher.get_request(name=self._name)

	def _save(self, request):
		result = self._execute(request)
		if result:
			msg = request.save(result)
			if self._prb:
				if msg:
					self._prb.set_description('{} {}'.format(msg,self._name))
				self._prb.update(1)
		else:
			if self._prb:
				self._prb.set_description("Couldn't execute request for {} using {}".format(request,self._name))
				self._prb.update(1)
			self._current_dispatcher.failed(self._name, request)

	@abstractmethod
	def _cool_down_time(self):
		raise NotImplementedError

	@abstractmethod
	def _execute(self, request):
		raise NotImplementedError

	def update(self, dispatcher):
		self._dispatcher_set.add(dispatcher)

	def set_progressbar(self, progressbar):
		self._prb = progressbar

class Schedule(_RequestExecutor):
	def __init__(self, executors, time_to_execution, thread_pool):  # wraps around a dispatcher
		super(Schedule,self).__init__('Schedule')
		self._time = time_to_execution
		self._executors = executors
		self._pool = thread_pool
		self._progressbar = None
		print('scheduled for execution in: ', time_to_execution, ' seconds')

	def __call__(self):
		sleep(self._time)
		for executor in self._executors:  # submit executors...
			if self._progressbar:
				executor.set_progressbar(self._progressbar)
			self._pool.submit(executor)

	def _execute(self, request):
		raise NotImplementedError

	def _cool_down_time(self):
		raise NotImplementedError

	def set_progressbar(self, progressbar):
		self._progressbar = progressbar

