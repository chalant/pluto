'''
1) create a hub client (url is provided in cli)
    1.1) download and cache resources (calendars, exchanges...)
2) create and launch dev server
2) create and launch test controller
'''


import sys

from os import path

from PyQt6 import QtWidgets, QtCore
from gui import backtesting

import click

from directory import utils

from dev import strategy_store

from pluto.setup import directory_setup as ds
from pluto.control.processes import in_memory
from pluto import examples
from pluto.data.bundles import test


@click.group()
def main():
    pass


@main.command()
def install_examples():
    print("Installing examples...")

    root_dir = ds.create_directory(path.expanduser('~/.pluto'))

    store = strategy_store.StrategyStore(root_dir)

    # copy examples into directory
    for name, ap in examples.EXAMPLE_MODULES.items():
        args, path_ = ap

        try:
            store.add_strategy(name, path_)
        except FileExistsError:
            pass

    test_bundle = test.TestBundle(
        root_dir)

    # ingest data...
    test_bundle.ingest()

    print("Done")


@main.command()
def dev():

    # todo: need to walk down the directory tree to get already existing
    #  files.

    root_dir = utils.DirectoryInterface(ds.create_directory(
        path.expanduser('~/.pluto')))

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

