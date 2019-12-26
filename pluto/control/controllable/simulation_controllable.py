from pluto.control.controllable import controllable
from pluto import algorithm

class SimulationControllable(controllable.Controllable):
    def __init__(self, simulation_blotter):
        super(SimulationControllable, self).__init__()
        self._blotter = simulation_blotter

    def _get_algorithm_class(self):
        return algorithm.TradingAlgorithm

    def _get_sessions(self, dt, sim_params):
        return self._sessions

    def _update_blotter(self, blotter, broker_data):
        pass

    def _update_account(self, blotter, main_account):
        pass





