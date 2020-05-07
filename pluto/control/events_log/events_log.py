from sqlalchemy import sql
import abc

from pluto.control.events_log import schema
from pluto.interface.utils import paths, db_utils
from pluto.coms.utils import conversions

from protos import (
    events_pb2,
    clock_pb2,
    controller_pb2,
    broker_pb2)

class NoopIterator(object):
    def __next__(self):
        raise StopIteration

    def __iter__(self):
        return self

class AbstractEventsLog(object):
    @abc.abstractmethod
    def writer(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def read(self, session_id, datetime):
        '''

        Parameters
        ----------
        session_id: str
        datetime: pandas.Timestamp

        Returns
        -------
        typing.Iterable[_Event]
        '''
        raise NotImplementedError()

class EventsLog(AbstractEventsLog):


    def __init__(self):
        root = paths.get_dir('control')
        dir_ = paths.get_dir('events', root)
        file_ = paths.get_file_path('metadata', dir_)

        self._engine = engine = db_utils.create_engine(file_)
        schema.metadata.create_all(engine)
        self._writer = _EventsLogWriter(engine, dir_)

    def writer(self):
        return self._writer

    def read(self, session_id, datetime):
        with self._engine.begin() as connection:
            statement = sql.select(
                [schema.datetimes.datetime,
                 schema.events.file_path]) \
                .join(schema.datetimes) \
                .where(schema.datetimes.c.datetime > datetime)
            result = connection.execute(statement)
            values = []
            for row in result:
                values.append(_Events(
                    row['datetime'],
                    row['file_path']))
            for evt in sorted(values):
                with open(evt.file_path, 'rb') as f:
                    for line in f.read():
                        event = events_pb2.Event()
                        event.ParseFromString(line)
                        evt_type = event.event_type
                        dt, evt = _create_event(evt_type, event.event)
                        if dt > datetime:
                            if evt_type == 'parameter':
                                # filter session id
                                if evt.session_id == session_id:
                                    yield evt
                            else:
                                yield evt_type, evt

class NoopEventsLog(object):
    def __init__(self):
        self._writer = _NoopEventLogWriter()
        self._itr = NoopIterator()

    def writer(self):
        return self._writer

    def read(self, session_id, datetime):
        return self._itr


class _NoopEventLogWriter(object):
    def initialize(self, datetime):
        pass

    def write_datetime(self, datetime):
        pass

    def write_event(self, event_type, event):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class _EventsLogWriter(object):
    def __init__(self, engine, root_dir):
        self._connection = None
        self._path = None
        self._session = None
        self._dir = root_dir
        self._engine = engine

    def initialize(self, datetime):
        '''

        Parameters
        ----------
        datetime: pandas.Timestamp

        '''

        # called once per session
        self._path = path = paths.get_file_path(
            str(datetime),
            self._dir)
        self._session = datetime
        self._connection.execute(
            schema.events.insert().values(
                session=datetime,
                file_path=path))

    def write_datetime(self, datetime):
        self._connection.execute(
            schema.datetimes.insert().values(
                datetime=datetime,
                session=self._session))

    def write_event(self, event_type, event):
        # store signals in a file if we have signals
        with open(self._path, 'wb') as f:
            f.write(events_pb2.Event(
                event_type=event_type,
                event=event.SerializeToString())
                    .SerializeToString())

    def __enter__(self):
        '''

        Returns
        -------
        _EventsLogWriter
        '''
        self._connection = self._engine.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._connection.close()

_noop_events_log = None
_events_log = None

def get_events_log(mode='simulation'):
    if mode == 'live':
        global _events_log
        if not _events_log:
            _events_log = EventsLog()
        return _events_log
    else:
        global _noop_events_log
        if not _noop_events_log:
            _noop_events_log = NoopEventsLog()
        return _noop_events_log

class _Events(object):
    __slots__ = ['datetime', 'file_path']

    def __init__(self, datetime, file_path):
        self.file_path = file_path
        self.datetime = datetime

    def __gt__(self, other):
        '''

        Parameters
        ----------
        other: _Events

        '''
        if self.datetime > other.datetime:
            return True
        else:
            return False

    def __lt__(self, other):
        '''

        Parameters
        ----------
        other: _Events

        '''
        if self.datetime < other.datetime:
            return True
        else:
            return False

    def __eq__(self, other):
        '''

        Parameters
        ----------
        other: _Events

        '''

        if self.datetime == other.datetime:
            return True

        else:
            return False

    def __ge__(self, other):
        return self.__eq__(other) or self.__gt__(other)

    def __le__(self, other):
        return self.__eq__(other) or self.__lt__(other)


class _Event(object):
    __slots__ = ['datetime', 'event']

    def __init__(self, datetime, event):
        self.datetime = datetime
        self.event = event


def _create_event(event_type, bytes_):
    if event_type == 'clock':
        event = clock_pb2.ClockEvent()
        event.ParseFromString(bytes_)
        return conversions.to_datetime(event.timestamp), event
    elif event_type == 'parameter':
        event = controller_pb2.RunParamsList()
        event.ParseFromString(bytes_)
        return conversions.to_datetime(event.timestamp), event
    elif event_type == 'broker':
        event = broker_pb2.BrokerState()
        event.ParseFromString(bytes_)
        return conversions.to_datetime(event.timestamp), event
