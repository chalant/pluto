from ui_backtesting import Ui_Backtesting

from PyQt6 import QtCore, QtGui, QtWidgets

if __name__ == '__main__':
    import sys

    app = QtWidgets.QApplication(sys.argv)
    Backtesting = QtWidgets.QWidget()
    ui = Ui_Backtesting()
    ui.setupUi(Backtesting)
    Backtesting.show()
    sys.exit(app.exec())