class TempDirectory(object):
    def __init__(self, dir_):
        self._directory = dir_

    @property
    def path(self):
        return self._directory.path

    def get_directory(self, name):
        res = self._directory.get_directory(name)

        if not res:
            raise FileNotFoundError('No directory named {}'.format(name))

        return res

    def get_file(self, name, extension):
        res = self._directory.get_file(name, extension)

        if res:
            return res

        dir_ = self._directory

        raise FileNotFoundError('Directory {} does not contain file {} of type {}'.format(dir_.name, name, extension))

    def __enter__(self):
        return self._directory

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._directory.delete()


class DirectoryInterface(object):
    def __init__(self, dir_):
        self._directory = dir_

    @property
    def path(self):
        return self._directory.path

    def get_directory(self, name):
        res = self._directory.get_directory(name)

        if not res:
            raise FileNotFoundError('No directory named {} in {}'.format(name, self._directory.name))

        return DirectoryInterface(res)

    def get_file(self, name, extension):
        res = self._directory.get_file(name, extension)

        if res:
            return res

        dir_ = self._directory

        raise FileNotFoundError('Directory {} does not contain file {} of type {}'.format(dir_.name, name, extension))


