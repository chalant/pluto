import threading

class ProcessManager(object):
    def __init__(self):
        self._active = {}
        self._recovering = {}

    def stop(self, session_id, liquidate=False):
        def stop(process, liquidate):
            process.stop(liquidate)

        threading.Thread(
            target=stop,
            args=(self._pop(session_id, self._active),
                liquidate)).start()

    @property
    def active_processes(self):
        return self._values(self._active)

    @property
    def active_session_ids(self):
        return self._keys(self._active)

    @property
    def all_session_ids(self):
        return self._get_all_session_ids(self._active, self._recovering)

    def recover(self, process):
        self._start_recovery(
            process,
            self._active,
            self._recovering)

    def add_process(self, process):
        self._add_process(process, self._active)

    def get_process(self, session_id):
        return self._get(self._active, session_id)

    def _get_all_session_ids(self, active, recovering):
        return active.keys() | recovering.keys()

    def _keys(self, dict_):
        return dict_.keys()

    def _values(self, dict_):
        return dict_.values()

    def _get(self, dict_, session_id):
        return dict_[session_id]

    def _pop(self, session_id, processes):
        return processes.pop(session_id)

    def _start_recovery(self, session_id, processes, recovering):
        #no recovery in default (simulation)
        pass

    def _add_process(self, process, active_processes):
        active_processes[process.session_id] = process


class LiveProcessManager(ProcessManager):
    def __init__(self):
        super(LiveProcessManager, self).__init__()
        self._lock = threading.Lock()

    def _get_all_session_ids(self, active, recovering):
        lock = self._lock
        lock.acquire()
        ids = active.keys() | recovering.keys()
        lock.release()
        return ids

    def _keys(self, dict_):
        lock = self._lock
        lock.acquire()
        values = dict_.keys()
        lock.release()
        return values

    def _values(self, dict_):
        lock = self._lock
        lock.acquire()
        values = dict_.keys()
        lock.release()
        return values

    def _get(self, dict_, session_id):
        lock = self._lock
        lock.acquire()
        prc = dict_[session_id]
        lock.release()
        return prc

    def _pop(self, session_id, processes):
        lock = self._lock
        lock.acquire()
        prc = processes.pop(session_id)
        lock.release()
        return prc

    def _add_process(self, process, active_processes):
        lock = self._lock
        lock.acquire()
        active_processes[process.session_id] = process
        lock.release()

    def _start_recovery(self, process, processes, recovering):
        def recover(process, processes, recovering, lock):
            session_id = process.session_id
            process.recover()
            lock.acquire()
            processes[process.session_id] = recovering.pop(session_id)
            lock.release()

        lock = self._lock
        session_id = process.session_id

        lock.acquire()
        processes.pop(session_id)
        recovering[session_id] = process
        lock.release()

        threading.Thread(
            target=recover,
            args=(
                process,
                processes,
                recovering,
                lock)).start()

