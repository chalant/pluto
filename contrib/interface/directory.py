import uuid

from os import path

from contrib.interface import interface as sch
from contrib.interface.utils import paths


class Strategy(object):
    def __init__(self, metadata):
        '''

        Parameters
        ----------
        metadata : sch.StrategyMetadata
        '''
        self._path = metadata.directory_path
        self._locked = metadata.locked
        self._metadata = metadata

    def get_implementation(self):
        with open(path.join(self._path, 'strategy.py'), 'rb') as f:
            return f.read()

    def store_implementation(self, file):
        if not self._locked:
            with open(path.join(self._path, 'strategy.py'), 'wb') as f:
                f.write(file)
        else:
            raise RuntimeError('Cannot overwrite a locked strategy')

    def lock(self):
        self._metadata.locked = True

    def get_performance(self, mode):
        if mode == 'live':
            pass
        elif mode == 'paper':
            pass

    def write_performance(self, mode, packet):
        pass

class Directory(object):
    def __init__(self, root_path, session_factory):
        '''

        Parameters
        ----------
        root_path : str
        session_factory :

        '''
        self._id_to_path = {}
        self._session = None
        self._sess_fct = session_factory
        self._root_path = root_path

    def add_strategy(self, name, strategy_id=None):
        '''

        Parameters
        ----------
        name : str
        strategy_id : str

        Returns
        -------
        Strategy
        '''
        # todo: need to store the mappings

        session = self._session

        if not session:
            raise RuntimeError('Cannot add a strategy outside of the directory scope')

        id_ = uuid.uuid4().hex
        pth = paths.get_dir(id_, self._root_path)

        str_meta = sch.StrategyMetadata(id=id_, name=name, directory_path=pth, locked=False)
        session.add(str_meta)

        if strategy_id:
            m = session.query(sch.StrategyMetadata).get(strategy_id)
            file = self._get_template(path.join(m.directory_path, 'strategy.py'))
        else:
            file = self._get_template()

        with open(path.join(pth, 'strategy.py'), mode='wb') as f:
            f.write(file)

        return Strategy(str_meta)

    def _get_template(self, path=None):
        if path:
            with open(path, 'rb') as f:
                return f.read()
        else:
            #todo: return a basic template with the necessary functions
            return b''

    def __enter__(self):
        self._session = self._sess_fct()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not exc_val:
            self._session.commit()
        else:
            self._session.rollback()
            self._session.close()
            self._session = None