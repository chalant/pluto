from functools import partial


class StateStorage(object):
    def __init__(self, storage_path, thread_pool):
        '''

        Parameters
        ----------
        storage_path: str
        thread_pool: concurrent.futures.ThreadPoolExecutor
        '''
        self._storage_path = storage_path
        self._thread_pool = thread_pool

    def _write(self, state):
        # todo: should we append instead of over-writing?
        with open(self._storage_path, 'wb') as f:
            f.write(state)

    def store(self, dt, controllable):
        self._thread_pool.submit(partial(
            self._write,
            state=controllable.get_state(dt)))

    def load_state(self):
        with open(self._storage_path, 'rb') as f:
            return f.read()


class NoStateStorage(object):
    def store(self, dt, controllable):
        pass