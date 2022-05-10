import os

from os import path
from os import system

_ROOT = path.expanduser('~./pluto')
system("attrib +h " + _ROOT)


def make_root_dir():
    try:
        os.mkdir(_ROOT)
    except FileExistsError as e:
        pass