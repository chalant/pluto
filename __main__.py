'''
1) create a hub client (url is provided in cli)
    1.1) download and cache resources (calendars, exchanges...)
2) create and launch dev server
2) create and launch test controller
'''


import sys
from directory import directory

from PyQt6 import QtWidgets
from GUI import backtesting


class MainWindow(QtWidgets.QWidget):
    def __init__(self):
        super(MainWindow, self).__init__()


class Model(object):
    def deploy(self):
        pass


if __name__ == "__main__":
    # todo: create directories if they do not exist...
    # todo: create database engine
    # todo: create strategy templates

    #create root directory
    directory.make_root_dir()

    app = QtWidgets.QApplication(sys.argv)

    #window = MainWindow()
    #window.show()

    bk = backtesting.BackTestingView(Model())

    bk.show()

    sys.exit(app.exec())