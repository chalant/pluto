import threading


from pluto.interface.utils import service_access

from pluto.coms.utils import conversions

from protos import interface_pb2 as itf


def write_perf(file_path, packet):
    with open(file_path, 'ab') as f:
        f.write(packet + b'END')


def read_perf(file_path):
    with open(file_path, 'rb') as f:
        packets = []
        size = 1024 * 64 #64 KB

        while True:
            buffer = f.read(size)
            if not buffer:
                if packets:
                    last = packets.pop()
                    if last:
                        yield last
                break
            else:
                try:
                    # combine the last packet with the current buffer
                    buffer = packets.pop() + buffer
                except IndexError:
                    pass
                packets.extend(buffer.split(b'END'))
                last_packet = packets.pop()
                for packet in packets:
                    if packet:
                        yield packet
                packets.clear()
                packets.append(last_packet)


class NoneObserver(object):
    def clear(self):
        pass

    def update(self, performance, end):
        pass

    def stream(self):
        pass


class Observer(object):
    def __init__(self, monitor_stub):
        self._stub = monitor_stub
        self._reload = True

    def clear(self):
        self._reload = True

    def update(self, performance, end, file_path, process_id):
        stub = self._stub

        if self._reload:
            self._stream(stub, process_id, file_path)
            self._reload = False

        service_access.invoke(
            stub.PerformanceUpdate,
            itf.Packet(
                packet=performance,
                session_id=process_id,
                end=end))

    def stream(self, file_path, process_id):
        stub = self._stub

        itr = iter(read_perf(file_path))
        try:
            n0 = next(itr)
            while True:
                try:
                    n1 = next(itr)
                    service_access.invoke(
                        stub.PerformanceUpdate,
                        itf.Packet(
                            packet=n0,
                            session_id=process_id))
                    n0 = n1
                except StopIteration:
                    service_access.invoke(
                        stub.PerformanceUpdate,
                        itf.Packet(
                            packet=n0,
                            session_id=process_id,
                            end=True))
                    break
        except StopIteration:
            pass

        # self._stream(self._stub, self._session_id)

    def _stream(self, stub, session_id, file_path):
        for packet in read_perf(file_path):
            service_access.invoke(
                stub.PerformanceUpdate,
                itf.Packet(
                    packet=packet,
                    session_id=session_id))


class NullPerformanceWriter(object):
    def __init__(self, monitor, process_id):
        self._monitor = monitor
        self._process_id = process_id

        self._observer = False

    def performance_update(self, performance, end):
        if self._observer:
            packet = conversions.to_proto_performance_packet(
                performance).SerializeToString()
            service_access.invoke(
                self._monitor.PerformanceUpdate,
                itf.Packet(
                    packet=packet,
                    session_id=self._process_id,
                    end=end))

    def observe(self, observer):
        self._observer = True

    def stop_observing(self):
        pass


class PerformanceWriter(object):
    # class for writing performance in some file...
    # todo: need to write to session_id and execution mode (live, paper, simulation)
    # live and paper cannot be over-written, only appended
    # todo: the writer can make a call-back to some observers
    # we also have a reader (which is a writer observer)
    # the reader reads the performance in the file and waits for updates from the writer.
    # the updates are read before getting written in the filesystem.
    def __init__(self, file_path, thread_pool, process_id):
        """
        Parameters
        ----------
        file_path: str
        thread_pool: concurrent.futures.ThreadPoolExecutor
        """

        self._path = file_path
        self._thread_pool = thread_pool
        self._process_id = process_id

        # self._none_observer = none = _NoneObserver()
        # self._observer = _Observer(monitor_stub, file_path, session_id)
        self._observer = None
        # self._current_observer = none
        self._ended = False

        self._lock = threading.Lock()

    def _write(self, performance, end, path):
        packet = conversions.to_proto_performance_packet(
            performance).SerializeToString()

        if self._observer:
            self._observer.update(
                packet,
                end,
                path,
                self._process_id)

        write_perf(path, packet)

    def performance_update(self, performance, end):
        # todo: we need to do a non-blocking write using queues?
        # self._thread_pool.submit(partial(
        #     self._write,
        #     performance=performance,
        #     end=end,
        #     path=self._path))

        with self._lock:
            self._write(performance, end, self._path)
            self._ended = end

    def observe(self, observer):
        with self._lock:
            self._observer = observer

            if self._ended:
                self._thread_pool.submit(
                    observer.stream,
                    self._path,
                    self._process_id)
                # observer.stream()
            else:
                # clear so that we can stream from the beginning
                observer.clear()

    def stop_observing(self):
        with self._lock:
            self._observer = None
