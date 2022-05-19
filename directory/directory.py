import os

from os import path


class _Directory(object):
    def __init__(self, path_, name, parent=None):
        self.path_ = path_
        self.name = name
        self._parent = parent
        self._children = {}
        self._files = {}

    @property
    def path(self):
        return self.path_

    @property
    def name(self):
        return self.name

    def is_root(self):
        return self._parent is None

    def add_file(self, name, extension):
        """

        Parameters
        ----------
        name
        extension

        Returns
        -------
        _File
        """

        fl = _File(name, extension, self)

        with open(fl.path, 'a'):
            pass

        self._files[name] = fl

        return fl

    def add_directory(self, name):
        """

        Parameters
        ----------
        name

        Returns
        -------
        _Directory
        """
        p = path.join(self.path_, name)

        d = _Directory(p, self)

        try:
            os.mkdir(p)

            self._children['name'] = d

        except FileExistsError as e:
            pass

        return d

    def get_directory(self, name):
        if name in self._children:
            return self._children[name]
        elif self._children:
            for child in self._children:
                dir_ = child.get_directory(name)

                if dir_:
                    return dir_
        else:
            return

    def get_file(self, name, extension):
        """

        Parameters
        ----------
        name
        extension

        Returns
        -------
        _File
        """
        id_ = name + extension

        if id_ in self._files:
            return [self._files[id_]]

        elif self._children:
            results = []

            for child in self._children:
                fl = child.get_file(name, extension)

                if fl:
                    results.append(fl)

            return results
        else:
            return

    def get_parent(self):
        """

        Returns
        -------
        _Directory
        """
        return self._parent


class _File(object):
    def __init__(self, name, extension, dir_):
        self.name = name
        self.extension = extension
        self.dir_ = dir_

    @property
    def path(self):
        path.join(self.dir_.path_, self.name + ".", self.extension)


def create_directory(path_, name, parent=None):
    try:
        os.mkdir(path_)
    except FileExistsError as e:
        pass

    return _Directory(path_, name, parent)
