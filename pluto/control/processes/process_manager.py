import threading

from collections import defaultdict, deque

from grpc import RpcError


class ProcessManager(object):
    def __init__(self):
        self._active = {}

    def stop(self, session_id, params):
        active = self._active
        self._execute_event(
            self._pop(session_id, active),
            params,
            active,
            self._stop)

    def _stop(self, process, params):
        process.stop(params)

    @property
    def all_session_ids(self):
        return self._get_all_session_ids(self._active)

    def add_process(self, process):
        self._put(process, self._active)

    def get_process(self, session_id):
        return self._get(self._active, session_id)

    def parameter_update(self, session_id, params):
        active = self._active
        self._execute_event(
            self._get(active, session_id),
            params,
            active,
            self._parameter_update)

    def _parameter_update(self, process, params):
        process.parameter_update(params)

    def _execute_event(self, process, params, active, event_fn):
        self._execute(event_fn, params, process, active)

    def clock_update(self, clock_event):
        active = self._active

        def clock_update(process, clock_event):
            process.clock_update(clock_event)

        for process in self._values(active):
            self._execute(clock_update, clock_event, process, active)

    def account_update(self, data):
        active = self._active

        def account_update(process, params):
            process.account_update(params)

        for process in self._values(active):
            self._execute(
                account_update,
                data,
                process,
                active)

    def _execute(self, fn, params, process, active):
        try:
            fn(process, params)
        except RpcError as e:
            raise RuntimeError(process.session_id, e)

    def _get_all_session_ids(self, active):
        return set(active.keys())

    def _keys(self, dict_):
        return dict_.keys()

    def _values(self, dict_):
        return dict_.values()

    def _get(self, dict_, session_id):
        return dict_.get(session_id, None)

    def _put(self, process, dict_):
        dict_[process.session_id] = process

    def _pop(self, session_id, dict_):
        return dict_.pop(session_id, None)


# fixme: what if we're unable to recover the process? (multiple attempts (say 3) failed)
class LiveProcessManager(ProcessManager):
    def __init__(self, recovery_error_callback):
        super(LiveProcessManager, self).__init__()
        self._recovering = {}
        self._events = defaultdict(deque)

        self._lock = threading.Lock()
        self._events_lock = threading.Lock()

        self._error_callback = recovery_error_callback

    def _stop(self, session_id, params):
        pass

    def _thread(self, fn, params, process, active):
        try:
            fn(process, params)
        except RpcError as e:
            events = self._events
            lock = self._events_lock

            lock.acquire()
            events[process.session_id].append((fn, params))
            lock.release()

            self._start_recovery(
                process,
                active,
                self._recovering,
                events,
                lock)

    def _execute(self, fn, params, process, active):
        # execute request within a thread
        threading.Thread(
            target=self._thread,
            args=(fn, params, process, active)
        ).start()

    def _execute_event(self, process, params, active, event_fn):
        if process:
            self._execute(event_fn, params, process, active)
        else:
            # process is in recovery
            lock = self._events_lock
            lock.acquire()
            self._events[process.session_id] = (event_fn, params)
            lock.release()

    def _get_all_session_ids(self, active):
        lock = self._lock
        lock.acquire()
        ids = active.keys() | self._recovering.keys()
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

    def _put(self, process, active_processes):
        lock = self._lock
        lock.acquire()
        active_processes[process.session_id] = process
        lock.release()

    def _start_recovery(self,
                        process,
                        active,
                        recovering,
                        events,
                        events_lock):
        try:
            lock = self._lock
            session_id = process.session_id
            lock.acquire()
            active.pop(session_id)
            recovering[session_id] = process
            lock.release()

            session_id = process.session_id
            process.recover()

            # check if there is some pending request.
            events_lock.acquire()
            requests = events.pop(session_id, None)
            events_lock.release()

            if requests:
                # remove process from recovering state
                lock.acquire()
                recovering.pop(session_id)
                lock.release()

                # fixme: calls might keep crashing... recursive calls?
                # max_recursions?
                for fn, params in requests:
                    fn(process, params)
            else:
                # put it back in the active processes
                lock.acquire()
                active[process.session_id] = recovering.pop(session_id)
                lock.release()

        except KeyError as e:
            # process is already in recovery
            pass
