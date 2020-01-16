from os import path, mkdir

_ROOT = path.expanduser('~/.pluto')

if not path.isdir(_ROOT):
    mkdir(_ROOT)

def with_root(func):
    def wrapper(name, root=None):
        if root:
            pth = path.join(root, name)
        else:
            pth = name
        pth = path.join(_ROOT, pth)
        func(pth, root)
    return wrapper

def root():
    return _ROOT

@with_root
def get_dir(name, root=None):
    if not path.isdir(name):
        mkdir(name)
    return name

@with_root
def get_file_path(name, root=None):
    if not path.isfile(name):
        #touch file if it does not exist
        with open(name, 'w'):
            pass
    return name

@with_root
def create_file(name, root=None):
    if path.isfile(name):
        raise FileExistsError
    with open(name, 'w') as f:
        pass
    return name



