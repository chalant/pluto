from sqlalchemy import sql

from pluto.control.events_log import schema
from pluto.interface.utils import paths, db_utils
from pluto.coms.utils import conversions

from protos import (
    events_pb2,
    clock_pb2,
    controller_pb2,
    broker_pb2)

ROOT = paths.get_dir('control')
DIR = paths.get_dir('events', ROOT)
FILE = paths.get_file_path('metadata', DIR)

engine = db_utils.create_engine(FILE)
schema.metadata.create_all(engine)


class _NoOpEventLogWriter(object):
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
    def __init__(self):
        self._connection = None
        self._path = None
        self._session = None

    def initialize(self, datetime):
        '''

        Parameters
        ----------
        datetime: pandas.Timestamp

        '''

        # called once per session
        self._path = path = paths.get_file_path(str(datetime), DIR)
        self._session = datetime
        self._connection.execute(
            schema.events.insert().values(
                datetime=datetime,
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
        self._connection = engine.connect()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._connection.close()


_writer = _EventsLogWriter()
_noop_writer = _NoOpEventLogWriter()


def writer(mode='simulation'):
    # we use a lock to avoid multiple datetime updates
    '''

    Returns
    -------
    _EventsLogWriter
    '''
    if mode == 'live':
        return _writer
    else:
        return _noop_writer


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


def read(session_id, datetime):
    '''

    Parameters
    ----------
    session_id: str
    datetime: pandas.Timestamp

    Returns
    -------
    typing.Iterable[_Event]
    '''

    with engine.begin() as connection:
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
