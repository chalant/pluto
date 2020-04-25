from os import path, mkdir

_ROOT = ''
_PATHS = {}


def root_is_set():
    return _ROOT != ''


def setup_root(root):
    global _ROOT
    _ROOT = root


def remove_root():
    _ROOT = ''


def with_root(func):
    def wrapper(name, root=None):
        if root:
            pth = path.join(root, name)
        else:
            pth = name
        if not _ROOT or not path.isdir(_ROOT):
            raise RuntimeError('Outside of directory context')
        else:
            pth = path.join(_ROOT, pth)
            # only attempt to create directory or path if not done yet
            paths = _PATHS.get(pth, None)
            if not paths:
                func(pth, root)
            else:
                _PATHS[pth] = pth
            return pth

    return wrapper


def root():
    if not _ROOT:
        raise RuntimeError('Outside of directory context')
    else:
        return _ROOT


@with_root
def get_dir(name, root=None):
    if not path.isdir(name):
        mkdir(name)


@with_root
def get_file_path(name, root=None):
    if not path.isfile(name):
        # touch file if it does not exist
        with open(name, 'w'):
            pass


@with_root
def create_file(name, root=None):
    if path.isfile(name):
        raise FileExistsError
    with open(name, 'w') as f:
        pass
