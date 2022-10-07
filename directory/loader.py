import os

from directory import directory


def _file_id(name, extension):
    return name + extension


class DirectoryTree(object):
    def __init__(self, path_, name, parent=None):
        self._path_ = path_
        self._name = name
        self._parent = parent
        self._children = {}
        self._files = {}

    @property
    def path(self):
        return self._path_

    def add_file(self, name, extension):
        self._files[_file_id(name, extension)] = directory.File(name, extension, self)

    def add_directory(self, name):
        d = DirectoryTree(
            os.path.join(self.path, name),
            name,
            parent=self)

        self._children[name] = d

        return d

    def clear_files(self):
        self._files.clear()

    def clear_directories(self):
        self._children.clear()

    def get_file(self, name, extension):
        id_ = _file_id(name, extension)

        if id_ in self._files:
            return self._files[id_]
        else:
            return

    def get_directory(self, name):
        if name in self._children:
            return self._children[name]
        else:
            return


def load_directory(dir_):
    '''

    Parameters
    ----------
    dir_: directory.Directory
    path_: str
    name: str

    Returns
    -------

    '''

    stack = [iter(os.listdir(dir_.path))]

    dir_stack = [dir_]

    while stack:
        itr = stack[-1]
        try:
            element = next(itr)
            current_dir = dir_stack[-1]
            path = os.path.join(
                current_dir.path,
                element)

            if os.path.isdir(path):
                d = current_dir.add_directory(element)
                stack.append(iter(os.listdir(d.path)))
                dir_stack.append(d)
            elif os.path.isfile(path):
                # print("Adding File", current_dir.path, element)
                name, extension = os.path.splitext(element)
                current_dir.add_file(name, extension.split('.')[-1])

        except StopIteration:
            if not stack:
                break

            stack.pop()
            dir_stack.pop()

            # for element in os.listdir(ld.path):
            #     if os.path.isfile(path):
            #         print("Adding File", path_, element)
            #         name, extension = os.path.splitext(element)
            #         current_dir.add_file(name, extension.split('.')[-1])

    return dir_
