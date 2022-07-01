import sys

from PyQt6.QtWidgets import *
from PyQt6.QtGui import *


# class CloseWidget(QTextEdit):
#     def closeEvent(self, QCloseEvent):
#         print("Closed!")
#
#
# class MainWindow(QMainWindow):
#     count = 0
#
#     def __init__(self, parent=None):
#         super(MainWindow, self).__init__(parent)
#         self.mdi = QMdiArea()
#         self.mdi.setDocumentMode(True)
#         self.mdi.setTabsMovable(False)
#
#         self.setCentralWidget(self.mdi)
#         bar = self.menuBar()
#
#         file = bar.addMenu("File")
#         file.addAction("New")
#         file.addAction("cascade")
#         file.addAction("Tiled")
#         file.triggered[QAction].connect(self.windowaction)
#
#         self.setWindowTitle("MDI demo")
#
#     def windowaction(self, q):
#         if q.text() == "New":
#             MainWindow.count = MainWindow.count + 1
#             sub = QMdiSubWindow()
#             sub.setWidget(CloseWidget())
#             sub.setWindowTitle("subwindow" + str(MainWindow.count))
#             self.mdi.addSubWindow(sub)
#             sub.show()
#
#         if q.text() == "cascade":
#             self.mdi.cascadeSubWindows()
#
#         if q.text() == "Tiled":
#             self.mdi.tileSubWindows()
#
# def hello():
#     print("Hello!")
#
# def main():
#     app = QApplication(sys.argv)
#     ex = MainWindow()
#     ex.show()
#     sys.exit(app.exec())
#
#
# if __name__ == '__main__':
#     main()
from PyQt6 import QtCharts, QtWidgets

import sys  # We need sys so that we can pass argv to QApplication
import os


class MainWindow(QtWidgets.QMainWindow):

    def __init__(self, *args, **kwargs):
        super(MainWindow, self).__init__(*args, **kwargs)

        self.setGeometry(50, 50, 500, 300)

        line = QtCharts.QLineSeries(self)

        # self.graphWidget.addItem(item)
        # self.graphWidget.addItem(it)

        hour = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        temperature = [30, 32, 34, 32, 33, 31, 29, 32, 35, 45]

        for i, j in zip(hour, temperature):
            line.append(i, j)

        chart = QtCharts.QChart()
        chart_view = QtCharts.QChartView(chart)

        chart.addSeries(line)
        # item = pg.PlotItem()
        chart.createDefaultAxes()

        # self.graphWidget = pg.PlotWidget(plotItem=item)
        self.setCentralWidget(chart_view)

        # chart.setMaximumHeight(80)
        # chart.setMaximumWidth(16)

        x, y = chart.axes()
        x.setMax(20)
        y.setMin(0)
        y.setMax(80)

        # chart.removeSeries(line)
        # line.replace(10, 45,  10, 80)
        line.append(10.5, 80)
        line.append(12, 62)
        line.append(13, 52)
        line.append(14, 66)
        line.append(15, 56)
        line.append(16, 28)
        line.append(17, 20)
        line.append(18, 36)
        line.append(19, 12)
        line.append(20, 21)


        # chart.addSeries(line)
        #
        # chart.createDefaultAxes()

        # chart_view.update()


        # plotting.updateItems()
        # plot data: x, y values
        # plotting.appendData(hour, temperature)


def main():
    app = QtWidgets.QApplication(sys.argv)
    main = MainWindow()
    main.show()
    sys.exit(app.exec())


if __name__ == '__main__':
    main()
