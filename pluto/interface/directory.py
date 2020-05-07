import uuid
from abc import ABC, abstractmethod
import os
import threading
import shutil
import tarfile

import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import orm

from zipline.testing import core

from pluto.interface.utils import paths
from pluto.interface.utils import db_utils
from pluto.setup import setup

_STREAM_CHUNK_SIZE = 64 * 1024

Base = declarative_base()


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
    cancel_policy = sa.Column(sa.String, nullable=False)


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
    __slots__ = ['_path', '_universe', '_context', '_id', '_stg_id', '_metadata']

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
        self._metadata = meta

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

    @property
    def cancel_policy(self):
        return self._metadata.cancel_policy

    def get_strategy(self, strategy):
        '''

        Parameters
        ----------
        strategy: _Strategy

        Returns
        -------
        bytes
        '''
        b = b''
        for bytes_ in strategy.get_implementation():
            b += bytes_
        return b


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
            if chunk_size != -1:
                while True:
                    data = f.read(chunk_size)
                    if not data:
                        break
                    yield data
            else:
                yield f.read()

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
    def __init__(self, session, directory):
        '''

        Parameters
        ----------
        session: sqlalchemy.orm.Session
        '''
        self._session = session
        self._closed = False
        self._lock = threading.Lock()
        self._directory = directory

    def get_session(self, session_id):
        '''

        Parameters
        ----------
        session_id

        Returns
        -------
        _Session
        '''
        return _Session(
            self._session.query(
                SessionMetadata)
                .get(session_id), self)

    def add_session(self, strategy_id, universe, data_frequency, look_back, cancel_policy):
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
        id_ = strategy_id + universe
        session = self._session
        sess_meta = session.query(SessionMetadata) \
            .filter(SessionMetadata.id == id_).one_or_none()

        if not sess_meta:
            pth = paths.get_dir(id_, self._directory)
            sess_meta = SessionMetadata(
                id=id_,
                strategy_id=strategy_id,
                directory=pth,
                data_frequency=data_frequency,
                universe=universe,
                look_back=look_back,
                cancel_policy=cancel_policy
            )
            session.add(sess_meta)

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

        for m in self._session.query(StrategyMetadata):
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
        return self._add_strategy(self._session, name, self._directory)

    @abstractmethod
    def _get_strategy(self, metadata, context):
        raise NotImplementedError

    @abstractmethod
    def _add_strategy(self, session, name, directory):
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

    def _add_strategy(self, session, name, directory):
        raise RuntimeError('Cannot add a strategy in read mode')

    def _close(self, session, exc_type, exc_val, exc_tb):
        pass


class _Write(_Mode):
    def __init__(self, session, directory):
        super(_Write, self).__init__(session, directory)

    def _get_strategy(self, metadata, context):
        return _WritableStrategy(metadata, context)

    def _add_strategy(self, session, name, directory):
        session = self._session

        id_ = uuid.uuid4().hex
        pth = paths.get_file_path(id_ + '.py', directory)

        stg_meta = StrategyMetadata(id=id_, name=name, file_path=pth, locked=False)
        session.add(stg_meta)

        session.flush()
        with open(pth, mode='wb') as f:
            f.write(self._get_template())

        return _WritableStrategy(stg_meta, self)

    def _get_template(self):
        # todo: return a basic template with the necessary functions
        return b'''
        def hello_world():
            print('Hello World!')
        '''

    def _close(self, session, exc_type, exc_val, exc_tb):
        if not exc_val:
            session.commit()
        else:
            session.rollout()
        session.close()


class StubDirectory(object):
    def __init__(self, root_dir):
        self._root_dir = root_dir
        self._reader = None

    def __enter__(self):
        if not paths.root_is_set():
            paths.setup_root(self._root_dir)
            strategies_dir = paths.get_dir('strategies')
            metadata_file = paths.get_file_path('metadata', strategies_dir)
            engine = db_utils.create_engine(metadata_file)
            self._session_factory = fct = db_utils.get_session_maker(engine)
            self._session = sess = fct()
            self._reader = _Read(sess, strategies_dir)
        return self

    def read(self):
        '''

        Returns
        -------
        _Read
        '''
        reader = self._reader
        if not reader:
            raise RuntimeError('Outside of scope')
        return reader

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._reader = None


class AbstractDirectory(ABC):
    def __init__(self):
        self._root_dir = None

        self._session = None
        self._reader = None

        def exploding_factory():
            raise RuntimeError('Outside of directory context')

        self._session_factory = exploding_factory
        self._strategy_dir = None

    def write(self):
        '''

        Returns
        -------
        _Write
        '''

        # we create a new writer with a new session for each write request.
        return _Write(self._session_factory(), self._strategy_dir)

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
        self._root_dir = root_dir = self._create_root_directory()
        if not os.path.isdir(root_dir):
            os.mkdir(root_dir)
        paths.setup_root(root_dir)
        setup.prepare_variables()
        # load setup parameters

        # todo: we need some values for 'default' setup. this is for testing
        # purposes
        with tarfile.open(os.path.join(
                core.zipline_git_root,
                'pluto/resources/test_setup.tar.gz')) as tar:
            tar.extractall(root_dir)

        self._strategy_dir = strategies_dir = paths.get_dir('strategies')
        metadata_file = paths.get_file_path('metadata', strategies_dir)
        engine = db_utils.create_engine(metadata_file)
        self._session_factory = fct = db_utils.get_session_maker(engine)
        self._session = sess = fct()
        self._reader = _Read(sess, strategies_dir)
        Base.metadata.create_all(engine)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._reader.close()
        self._session.close()
        self._exit(self._root_dir)
        paths.remove_root()

    @abstractmethod
    def _create_root_directory(self):
        raise NotImplementedError('_create_root_directory')

    @abstractmethod
    def _exit(self, root_dir):
        raise NotImplementedError('_exit')


class _Directory(AbstractDirectory):
    def _create_root_directory(self):
        return os.path.expanduser('~/.pluto')

    def _exit(self, root_dir):
        pass


class _TempDirectory(AbstractDirectory):
    def _create_root_directory(self):
        return os.path.expanduser('~/tmp/pluto')

    def _exit(self, root_dir):
        # delete everything
        shutil.rmtree(root_dir, ignore_errors=True)


_DIRECTORY = None


def get_directory(environment):
    global _DIRECTORY
    if not _DIRECTORY:
        if environment == 'test':
            _DIRECTORY = _TempDirectory()
        else:
            _DIRECTORY = _Directory()
    return _DIRECTORY
