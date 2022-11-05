from PyQt6 import QtWidgets, QtCore


class Emitter(QtCore.QObject):
    text_written = QtCore.pyqtSignal(str)

    def write(self, text):
        self.text_written.emit(str(text))


class Log(QtWidgets.QWidget):
    def __init__(self, parent):
        super(Log, self).__init__(parent)

        layout = QtWidgets.QHBoxLayout()

        self.setLayout(layout)

        self._text_edit = text_edit = QtWidgets.QTextEdit(self)

        layout.addWidget(text_edit)

    def write(self, text):
        cursor = self._text_edit.textCursor()
        cursor.movePosition(cursor.MoveOperation.End)
        cursor.insertText(text)

        self._text_edit.setTextCursor(cursor)
        self._text_edit.ensureCursorVisible()

    def reset(self):
        self._text_edit.clear()

