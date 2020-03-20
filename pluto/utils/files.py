import os

from abc import ABC, abstractmethod

import json

from zipline.utils import paths

class _File(ABC):
    #TODO: create the directory in the constructor
    def __init__(self, name, environ=None):
        self._dirs = []
        self._path = self._get_contrib_root(environ)
        rp, file = os.path.split(name)
        self._abs_path = os.path.join(self._create_dir(rp), file)

    def store(self, data):
        self._store(self._abs_path, data)

    def _create_dir(self, rel_path):
        t = os.path.split(rel_path)
        root = t[0]
        dir_ = t[1]
        if not root:
            # we hit bottom, create root directory
            p = os.path.join(self._path,dir_)
            self._make_dir(p)
            for d in self._dirs:
                dir_ = os.path.join(p, d)
                self._make_dir(dir_)
            return dir_
        else:
            self._dirs.append(dir_)
            return self._create_dir(root)

    def _make_dir(self,path):
        try:
            os.mkdir(path)
        except FileExistsError:
            pass

    def load(self):
        return self._load(self._abs_path)

    @abstractmethod
    def _load(self, path):
        raise NotImplementedError

    @abstractmethod
    def _store(self, path, data):
        raise NotImplementedError

    def _get_contrib_root(self, environ=None):
        zp_root = paths.zipline_root(environ)
        root = os.path.join(zp_root, 'contrib')
        paths.ensure_directory(root)
        return root


class AppendTextFile(_File):
    def _store(self, path, data):
        with open(path, 'a') as f:
            f.write(data + '\n')

    def load(self):
        with open(self._path) as f:
            return f.readlines()


class OverwriteTextFile(_File):
    def _store(self, path, data):
        with open(path, 'w') as f:
            f.write(data)

    def _load(self, path):
        with open(path) as f:
            yield f.read()


class OverwriteByteFile(_File):
    def _store(self, path, data):
        with open(path, 'wb') as f:
            f.write(data)

    def _load(self, path):
        with open(path, 'rb') as f:
            yield f.read()


class AppendByteFile(_File):
    def _store(self, path, data):
        with open(path, 'ab') as f:
            f.write(data + '\n')

    def _load(self, path):
        with open(path, 'rb') as f:
            return f.readlines()


class JsonFile(_File):
    def _store(self, path, data):
        if not isinstance(data, dict):
            raise TypeError
        try:
            with open(path + '.json') as f:
                j = json.loads(f.read())
            with open(path + '.json', 'w') as f:
                f.write(json.dumps({**j, **data}))
        except FileNotFoundError:
            with open(path + '.json', 'w') as f:
                f.write(json.dumps(data))

    def _load(self, path):
        with open(path + '.json') as f:
            yield json.loads(f.read())
