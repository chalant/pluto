'''
1) create a hub client (url is provided in cli)
    1.1) download and cache resources (calendars, exchanges...)
2) create and launch dev server
2) create and launch test controller
'''


import sys

from os import path

from PyQt6 import QtWidgets
from GUI import backtesting

import click

from directory import utils

from pluto.setup import directory_setup as ds
from pluto.control.processes import in_memory


class MainWindow(QtWidgets.QWidget):
    def __init__(self):
        super(MainWindow, self).__init__()


class Model(object):
    def deploy(self):
        pass


@click.group()
def main():
    pass


@main.command()
def dev():
    # todo: copy examples to directory...
    #  we will use them for testing etc.
    # create directories if they do not exist...
    root_dir = utils.DirectoryInterface(ds.create_directory(path.expanduser('~/.pluto')))

    app = QtWidgets.QApplication(sys.argv)

    bk = backtesting.BackTesting(
        root_dir,
        in_memory.InMemoryControllableStub(
            root_dir,
            'backtesting'))

    bk.show()

    sys.exit(app.exec())


@main.command()
def account():
    pass


if __name__ == "__main__":
    # todo: directories should be created during installation, not when we launch
    #  the application...
    main()

