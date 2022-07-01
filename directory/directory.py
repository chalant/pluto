import os

from os import path

from shutil import rmtree


def _file_id(name, extension):
    return name + extension


class Directory(object):
    def __init__(self, path_, name, parent=None):
        self._path_ = path_
        self._name = name
        self._parent = parent
        self._children = {}
        self._files = {}

    @property
    def path(self):
        return self._path_

    @property
    def name(self):
        return self._name

    def is_root(self):
        return self._parent is None

    def delete(self):
        rmtree(self.path)

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

        id_ = _file_id(name, extension)

        if id_ not in self._files:
            fl = File(name, extension, self)

            with open(fl.path, 'a'):
                pass

            self._files[id_] = fl

            return fl

        return self._files[id_]

    def add_directory(self, name):
        """

        Parameters
        ----------
        name

        Returns
        -------
        Directory
        """
        p = path.join(self.path, name)

        d = Directory(p, name, parent=self)

        try:
            os.mkdir(p)
        except FileExistsError as e:
            #
            for f in os.listdir(p):
                if os.path.isfile(os.path.join(p, f)):
                    fn, extension = f.split('.')
                    d.add_file(fn, extension)

        self._children[name] = d

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
        else:
            return

        # elif self._children:
        #     for child in self._children:
        #         dir_ = child.get_directory(name)
        #
        #         if dir_:
        #             return dir_
        # else:
        #     return

    def get_file(self, name, extension):
        """

        Parameters
        ----------
        name
        extension

        Returns
        -------
        Fileimport datetime
        """

        id_ = _file_id(name, extension)

        if id_ in self._files:
            return self._files[id_]
        else:
            return

        # elif self._children:
        #     results = []
        #
        #     for child in self._children:
        #         fl = child.get_file(name, extension)
        #
        #         if fl:
        #             results.append(fl)
        #
        #     return results
        # else:
        #     return

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
        return path.join(self.dir_.path, self.name + "." + self.extension)


def create_directory(path_, name, parent=None):
    try:
        os.mkdir(path_)
    except FileExistsError as e:
        pass

    return Directory(path_, name, parent)
