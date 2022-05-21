import os

from os import path
from os import rmdir


class Directory(object):
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

    def delete(self):
        rmdir(self.path)

        self._children.clear()
        self._files.clear()

    def add_file(self, name, extension):
        """

        Parameters
        ----------
        name
        extension

        Returns
        -------
        File
        """

        fl = File(name, extension, self)

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
        Directory
        """
        p = path.join(self.path_, name)

        d = Directory(p, self)

        try:
            os.mkdir(p)

            self._children[name] = d

        except FileExistsError as e:
            pass

        return d

    def get_directory(self, name):
        """

        Parameters
        ----------
        name

        Returns
        -------
        Directory
        """

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
        File
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
        Directory
        """
        return self._parent


class File(object):
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

    return Directory(path_, name, parent)
