import threading

class Yo(object):
    def __init__(self):
        self._stop = False
        self._thread = None
        self._event = threading.Event()
        self._running = False

    def _run(self):
        while not self._stop:
            print('Yo')
            self._event.wait(0.5)

    def run(self):
        if not self._running:
            self._event.clear()
            self._stop = False
            self._thread = threading.Thread(target=self._run)
            self._thread.start()
            self._running = True
        else:
            pass

    def stop(self):
        if self._running:
            self._stop = True
            self._running = False
            self._event.set()
        else:
            pass
