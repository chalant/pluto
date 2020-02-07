import uuid

from os import path
from abc import ABC, abstractmethod

import threading

import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import orm

from pluto.interface.utils import paths
from pluto.interface.utils import db_utils

_STREAM_CHUNK_SIZE = 64 * 1024
_DIRECTORY = paths.get_dir('strategies')
_METADATA_FILE = paths.get_file_path('metadata', _DIRECTORY)

Base = declarative_base()
engine = db_utils.create_engine(_METADATA_FILE)
DBSession = db_utils.get_session_maker(engine)

def _check_scope(func):
    def wrapper(instance, *args, **kwargs):
        '''

        Parameters
        ----------
        instance : _Mode
        args
        kwargs

        Returns
        -------
        function
        '''
        if instance.closed:
            raise RuntimeError('cannot perform action outside of scope!')
        return func(instance, *args, **kwargs)

    return wrapper


class StrategyMetadata(Base):
    __tablename__ = 'strategies_metadata'

    id = sa.Column(sa.String, primary_key=True, nullable=False)
    name = sa.Column(sa.String)
    file_path = sa.Column(sa.String, nullable=False)
    locked = sa.Column(sa.Boolean, nullable=False)
    sessions = orm.relationship("SessionMetadata")


class SessionMetadata(Base):
    __tablename__ = 'sessions_metadata'

    id = sa.Column(sa.String, primary_key=True, nullable=False)
    strategy_id = sa.Column(sa.String, sa.ForeignKey(StrategyMetadata.id))
    data_frequency = sa.Column(sa.String, nullable=False)
    directory = sa.Column(sa.String, nullable=False)
    universe = sa.Column(sa.String, nullable=False)
    look_back = sa.Column(sa.Integer)


class Scoped(object):
    def __init__(self, context):
        '''

        Parameters
        ----------
        context: _Mode
        '''
        self._context = context

    @property
    def closed(self):
        return self._context.closed

class _Session(Scoped):
    __slots__ = ['_path', '_universe', '_context', '_id', '_stg_id']

    def __init__(self, meta, context):
        '''

        Parameters
        ----------
        meta: SessionMetadata
        context: _Mode
        '''
        super(_Session, self).__init__(context)
        self._path = meta.directory
        self._universe = meta.universe
        self._context = context
        self._id = meta.id
        self._stg_id = meta.strategy_id
        self._look_back = meta.look_back
        self._data_frequency = meta.data_frequency

    @property
    def look_back(self):
        return self._look_back

    @property
    def data_frequency(self):
        return self._data_frequency

    @property
    def id(self):
        return self._id

    @property
    def universe_name(self):
        return self._universe

    @property
    def strategy_id(self):
        return self._stg_id

    def get_strategy(self, chunk_size=_STREAM_CHUNK_SIZE):
        with open(self._stg_id + '.py', 'rb') as f:
            while True:
                data = f.read(chunk_size)
                if not data:
                    break
                yield data


class _Strategy(Scoped):
    def __init__(self, metadata, context):
        '''

        Parameters
        ----------
        metadata: StrategyMetadata
        context: _Mode
        '''
        super(_Strategy, self).__init__(context)
        self._path = metadata.file_path
        self._locked = metadata.locked
        self._name = metadata.name
        self._id = metadata.id
        self._metadata = metadata
        self._context = context

    @property
    def locked(self):
        return self._locked

    @property
    def mode(self):
        return self._mode

    @property
    def name(self):
        return self._name

    @property
    def id(self):
        return self._id

    @property
    def path(self):
        return self._path

    def get_implementation(self, chunk_size=_STREAM_CHUNK_SIZE):
        with open(self._path, 'rb') as f:
            while True:
                data = f.read(chunk_size)
                if not data:
                    break
                yield data

    def store_implementation(self, bytes_):
        '''

        Parameters
        ----------
        bytes_ : typing.Iterable[bytes]

        Returns
        -------
        None
        '''
        self._store(self._metadata, bytes_)

    def _store(self, metadata, bytes):
        raise NotImplementedError

    @_check_scope
    def lock(self):
        self._lock(self._metadata)

    def _lock(self, metadata):
        pass


class _WritableStrategy(_Strategy):
    def _lock(self, metadata):
        metadata.locked = True

    def _store(self, metadata, bytes_):
        if not self._locked:
            with open(self._path, 'wb') as f:
                f.write(bytes_)
        else:
            raise RuntimeError('Cannot overwrite a locked strategy')


class _Mode(ABC):
    def __init__(self, session):
        '''

        Parameters
        ----------
        session: sqlalchemy.orm.Session
        '''
        self._session = session
        self._closed = False
        self._lock = threading.Lock()

    def get_session(self, session_id):
        return _Session(
            self._session.query(
                SessionMetadata)
                .get(session_id), self)

    def add_session(self, strategy_id, universe, data_frequency, look_back):
        '''

        Parameters
        ----------
        strategy_id: str
        universe: str

        Returns
        -------
        _Session
        '''

        # we use the args pair as key
        id_ = strategy_id+universe
        session = self._session
        sess_meta = session.query(SessionMetadata) \
            .filter(SessionMetadata.id == id_).one_or_none()

        if not sess_meta:
            pth = paths.get_dir(id_, _DIRECTORY)
            sess_meta = SessionMetadata(
                id=id_,
                strategy_id=strategy_id,
                directory=pth,
                data_frequency=data_frequency,
                universe=universe,
                look_back=look_back)
            session.add(sess_meta)
            print('Done!')

        return _Session(sess_meta, self)

    @property
    def closed(self):
        with self._lock:
            return self._closed

    @_check_scope
    def get_strategy(self, strategy_id):
        '''

        Parameters
        ----------
        strategy_id : str

        Returns
        -------
        _Strategy
        '''
        return self._get_strategy(
            self._session.query(StrategyMetadata)
                .get(strategy_id), self)

    @_check_scope
    def get_strategy_list(self):
        '''

        Returns
        -------
        typing.Generator[_Strategy]
        '''

        print('Fetching list...')
        for m in self._session.query(StrategyMetadata):
            yield self._get_strategy(m, self)
        print('Done')

    @_check_scope
    def add_strategy(self, name):
        '''

        Parameters
        ----------
        name : str
        strategy_id : str

        Returns
        -------
        _Strategy
        '''
        return self._add_strategy(self._session, name)

    @abstractmethod
    def _get_strategy(self, metadata, context):
        raise NotImplementedError

    @abstractmethod
    def _add_strategy(self, session, name):
        raise NotImplementedError

    @abstractmethod
    def _close(self, session, exc_type, exc_val, exc_tb):
        raise NotImplementedError

    def close(self):
        with self._lock:
            if not self._closed:
                self._closed = True  # set the flag before closing
                self._close(self._session, None, None, None)

    def __enter__(self):
        with self._lock:
            self._closed = False
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # wait for any ongoing transactions
        self.close()


class _Read(_Mode):
    def _get_strategy(self, metadata, context):
        return _Strategy(metadata, context)

    def _add_strategy(self, session, name):
        raise RuntimeError('Cannot add a strategy in read mode')

    def _close(self, session, exc_type, exc_val, exc_tb):
        pass


class _Write(_Mode):
    def __init__(self, session):
        super(_Write, self).__init__(session)

    def _get_strategy(self, metadata, context):
        return _WritableStrategy(metadata, context)

    def _add_strategy(self, session, name):
        session = self._session

        id_ = uuid.uuid4().hex
        pth = paths.get_file_path(id_+'.py', _DIRECTORY)

        stg_meta = StrategyMetadata(id=id_, name=name, file_path=pth, locked=False)
        session.add(stg_meta)

        session.flush()

        with open(pth, mode='wb') as f:
            f.write(self._get_template())

        return _WritableStrategy(stg_meta, self)

    def _get_template(self):
        # todo: return a basic template with the necessary functions
        return b'Hello World!!!'

    def _close(self, session, exc_type, exc_val, exc_tb):
        if not exc_val:
            session.commit()
        else:
            session.rollout()
        session.close()


class Directory(object):
    def __init__(self):
        self._session = sess = DBSession()
        self._reader = _Read(sess)
    def write(self):
        '''

        Returns
        -------
        _Write
        '''

        # we create a new writer with a new session for each write request.
        return _Write(DBSession())

    def read(self):
        '''

        Returns
        -------
        _Read
        '''
        # we always return the same reader.
        return self._reader

    def open(self):
        return self.__enter__()

    def close(self):
        self.__exit__(None, None, None)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # blocks until the reader is released by another thread.
        self._reader.close()
        self._session.close()

Base.metadata.create_all(engine)