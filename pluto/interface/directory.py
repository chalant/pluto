import uuid

from os import path
from abc import ABC, abstractmethod

import threading

import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base

from pluto.interface.utils import paths
from pluto.interface.utils import db_utils

_DIRECTORY = 'Strategies'
_METADATA_FILE = 'metadata'

metadata = sa.MetaData()
Base = declarative_base(metadata=metadata)
engine = db_utils.create_engine(_METADATA_FILE, metadata, _DIRECTORY)
Session = db_utils.get_session_maker(engine)

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
    __tablename__ = 'directory_metadata'

    id = sa.Column(sa.String, primary_key=True, nullable=False)
    name = sa.Column(sa.String)
    directory_path = sa.Column(sa.String, nullable=False)
    locked = sa.Column(sa.Boolean, nullable=False)


class _Strategy(object):
    _STREAM_CHUNK_SIZE = 64 * 1024

    def __init__(self, metadata, context):
        '''

        Parameters
        ----------
        metadata: sch.StrategyMetadata
        context: _Mode
        '''
        self._path = metadata.directory_path
        self._locked = metadata.locked
        self._name = metadata.name
        self._id = metadata.id
        self._metadata = metadata
        self._context = context

    @property
    def locked(self):
        return self._locked

    @property
    def closed(self):
        return self._context.closed

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
        return path.join(self._path, 'strategy.py')

    def get_implementation(self, chunk_size=_STREAM_CHUNK_SIZE):
        with open(path.join(self._path, 'strategy.py'), 'rb') as f:
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

    #todo
    def get_performance(self, mode):
        if mode == 'live':
            pass
        elif mode == 'paper':
            pass

    def write_performance(self, mode, packet):
        self._write_performance(metadata, mode, packet)

    #todo
    def _write_performance(self, metadata, mode, packet):
        raise NotImplementedError


class _WritableStrategy(_Strategy):
    def _lock(self, metadata):
        metadata.locked = True

    def _store(self, metadata, bytes_):
        if not self._locked:
            with open(path.join(self._path, 'strategy.py'), 'wb') as f:
                f.write(bytes_)
        else:
            raise RuntimeError('Cannot overwrite a locked strategy')

    #todo
    def _write_performance(self, metadata, mode, packet):
        raise NotImplementedError

class _Mode(ABC):
    def __init__(self, session):
        '''

        Parameters
        ----------
        session: sqlalchemy.orm.Session
        '''
        self._session = session
        self._closed = True
        self._lock = threading.Lock()

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
        for m in self._session.query(StrategyMetadata).all():
            yield self._get_strategy(m, self)

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
    def _enter(self):
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
            if self._closed:
                self._enter()
                self._closed = False
                return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        #wait for any ongoing transactions
        with self._lock:
            self.close()

class _Read(_Mode):
    def _get_strategy(self, metadata, context):
        return _Strategy(metadata, context)

    def _add_strategy(self, session, name):
        raise RuntimeError('Cannot add a strategy in read mode')

    def _enter(self):
        pass

    def _close(self, session, exc_type, exc_val, exc_tb):
        pass


class _Write(_Mode):
    def __init__(self, session, directory):
        super(_Write, self).__init__(session)
        self._root_path = directory

    def _get_strategy(self, metadata, context):
        return _WritableStrategy(metadata, context)

    def _add_strategy(self, session, name):
        session = self._session

        id_ = uuid.uuid4().hex
        pth = paths.get_dir(id_, self._root_path)

        stg_meta = StrategyMetadata(id=id_, name=name, directory_path=pth, locked=False)
        session.add(stg_meta)

        with open(path.join(pth, 'strategy.py'), mode='wb') as f:
            f.write(self._get_template())

        return _WritableStrategy(metadata, self)

    #todo
    def _get_template(self):
        # todo: return a basic template with the necessary functions
        return b''

    def _close(self, session, exc_type, exc_val, exc_tb):
        if not exc_val:
            session.commit()
        else:
            session.rollout()
        session.close()


class Directory(object):
    def __init__(self):
        self._session = session = Session()
        self._reader = _Read(session)

    def write(self):
        '''

        Returns
        -------
        _Write
        '''

        #we create a new writer with a new session for each write request.
        return _Write(Session(), _DIRECTORY)

    def read(self):
        '''

        Returns
        -------
        _Read
        '''
        #we always return the same reader.
        return self._reader

    def open(self):
        return self.__enter__()

    def close(self):
        self.__exit__(None, None, None)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        #blocks until the reader is released by another thread.
        self._reader.close()
        self._session.close()
