from pluto.control.controllable import controllable
from pluto.finance.blotter import liveblotter
from pluto import algorithm

class LiveControllable(controllable.Controllable):
    def __init__(self, broker_stub, state_storage_path):
        super(LiveControllable, self).__init__(state_storage_path)
        self._broker = broker_stub

    def _get_algorithm_class(self):
        return algorithm.LiveTradingAlgorithm

    def _session_start(self, dt, calendar, sims_params):
        #update parameters
        sims_params.start_session = calendar.next_close(sims_params.start_session).normalize()
        sims_params.end_session = dt

    def _update_blotter(self, blotter, broker_data):
        blotter.update(broker_data)

    def _create_blotter(self):
        return liveblotter.LiveBlotter(self._broker)

    def _update_account(self, blotter, main_account):
        blotter.update_account(main_account)
