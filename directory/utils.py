class TempDirectory(object):
    def __init__(self, dir_):
        self._directory = dir_

    @property
    def path(self):
        return self._path_

    def get_directory(self, name):
        res = self._dir.get_directory(name)

        if not res:
            raise FileNotFoundError('No directory named {}'.name)

        return res

    def get_file(self, name, extension):
        res = self._dir.get_file(name, extension)

        if res:
            return res

        dir_ = self._dir

        raise FileNotFoundError('Directory {} does not contain file {} of type {}'.format(dir_.name, name, extension))

    def __enter__(self):
        return self._directory

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._directory.delete()


class DirectoryInterface(object):
    def __init__(self, dir_):
        """

        Parameters
        ----------
        dir_: directory.directory.Directory
        """

        self._dir = dir_

    @property
    def path(self):
        return self._path_

    def get_directory(self, name):
        res = self._dir.get_directory(name)

        if not res:
            raise FileNotFoundError('No directory named {}'.name)

        return res

    def get_file(self, name, extension):
        res = self._dir.get_file(name, extension)

        if res:
            return res

        dir_ = self._dir

        raise FileNotFoundError('Directory {} does not contain file {} of type {}'.format(dir_.name, name, extension))


