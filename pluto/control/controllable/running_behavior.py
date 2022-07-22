import threading

import queue

from pluto.control.controllable import commands


class NonBlocking(object):
    def __init__(self):
        self._thread = None
        self._queue = queue.Queue()

        self._state = None

    def set_state(self, state):
        self._state = state

    def run(self):
        self._thread = thread = threading.Thread(target=self._run)
        thread.start()

    def execute(self, command):
        self._queue.put(command)

    def _run(self):
        q = self._queue

        while True:
            try:
                self._state.execute(q.get())
            except commands.StopExecution:
                break


class Blocking(object):
    def __init__(self):
        self._state = None

    def set_state(self, state):
        self._state = state

    def run(self):
        pass

    def execute(self, command):
        try:
            self._state.execute(command)
        except commands.StopExecution:
            pass
