from PyQt6 import QtWidgets


# todo: we need to use a template from our database
class ComboBox(QtWidgets.QComboBox):
    def __init__(self, parent):
        super(ComboBox, self).__init__(parent)

        self.currentIndexChanged.connect(self._on_selected)

        self._item_store = None

    def set_item_store(self, store):
        self._item_store = store
        combo = self

        combo.addItems(store.item_names)
        combo.setCurrentIndex(store.current_item_index)

    def _on_selected(self, index):
        self._item_store.current_element_index = index

    @property
    def value(self):
        return self._item_store.item_names[self.currentIndex()]

