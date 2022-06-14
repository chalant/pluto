from PyQt6 import QtWidgets

from GUI import ui_new_dialog

from dev import events

events.add_event('strategy.new')


class NewStrategy(QtWidgets.QDialog):
    def __init__(self, store, template_path):
        """

        Parameters
        ----------
        store: dev.strategy_store.StrategyStore
        """
        super(NewStrategy, self).__init__()

        self._ui = ui = ui_new_dialog.Ui_NewStrategy()

        ui.setupUi(self)

        self._accepted = ui.buttonBox.accepted
        self.close()

        ui.buttonBox.setEnabled(False)

        ui.name_input.textChanged.connect(
            self._on_name_input)

        self._added = events.Event()
        self._strategy_store = store

        self._template_path = template_path

    @property
    def added_event(self):
        return self._added

    def accept(self):
        """

        create a new strategy... this will create a file (based on template) and
        generate a session id ( a session groups a strategy with its parameters)
        generates a strategy id which is unique...

        once we deploy a strategy, we deploy it into a session where it will run
        with the specified parameters...
        a session also has an id that is a combination of all parameters using md5 hashing
        and concatenating the parameters strings.

        we can always overwrite the strategy parameters

        in development, we always run strategies by there names, and we
        can change the parameters as much as we want... once we've deployed a
        strategy it is "frozen" into a session. And in live mode, we run strategies
        by session.

        """
        store = self._strategy_store

        name = self._ui.name_input.text()

        events.post_event(
            "strategy.new",
            store.add_strategy(
                name,
                self._template_path))

        self.close()

    def reject(self):
        # reject request
        self.close()

    def _on_name_input(self):
        ui = self._ui
        accepted = ui.buttonBox

        name = ui.name_input.text()

        if (not name or self._strategy_store.contains_strategy_of_name(name)) and accepted.isEnabled():
            # todo: display that strategy already exists...
            accepted.setEnabled(False)
        else:
            accepted.setEnabled(True)
