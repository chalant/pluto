from PyQt6 import QtWidgets

from GUI import ui_load_dialog

from dev import events

events.add_event('strategy.loaded')


class LoadStrategy(QtWidgets.QDialog):
    def __init__(self, store):
        """

        Parameters
        ----------
        store: dev.strategy_store.StrategyStore
        """
        super(LoadStrategy, self).__init__()
        self._strategy_store = store

        self._ui = ui = ui_load_dialog.Ui_Dialog()

        self.setWindowTitle("Load Strategy")

        ui.setupUi(self)

        self._items = items = [stg.strategy_name for stg in store.get_strategies()]

        # add items from store
        ui.name_list.addItems(items)

        ui.name_list.currentIndexChanged.connect(self._on_selected)

        self._ok_button = ui.buttonBox.accepted

        ui.buttonBox.setEnabled(False)

        if not items:
            ui.name_list.setEnabled(False)

        self._selected = None

        events.subscribe("strategy.new", self._on_new_strategy)

    def accept(self):
        if self._selected:
            # trigger
            events.post_event("strategy.loaded", self._selected)
            self._selected = None

        super(LoadStrategy, self).accept()

    def _on_selected(self, index):
        self._selected = self._strategy_store.get_strategy_at_index(index)
        ui = self._ui

        ui.buttonBox.setEnabled(True)

    def _on_new_strategy(self, meta):
        self._items.append(meta)
        self._ui.name_list.addItem(meta.name)
        self._ui.name_list.setEnabled(True)

