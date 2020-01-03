from zipline.finance.blotter import simulation_blotter

from pluto.control.controllable import controllable
from pluto import algorithm

class SimulationControllable(controllable.Controllable):
    def __init__(self):
        super(SimulationControllable, self).__init__()

    def _get_algorithm_class(self,
                             params,
                             data_portal,
                             blotter,
                             metrics_tracker,
                             get_pipeline_loader,
                             initialize,
                             before_trading_start,
                             handle_data,
                             analyze):
        '''
        Returns
        -------
        pluto.algorithm.TradingAlgorithm
        '''
        return algorithm.TradingAlgorithm(
            params,
            data_portal,
            blotter,
            metrics_tracker,
            get_pipeline_loader,
            initialize,
            before_trading_start,
            handle_data,
            analyze
        )

    def _get_sessions(self, dt, sim_params):
        return self._sessions

    def _update_blotter(self, blotter, broker_data):
        pass

    def _update_account(self, blotter, main_account):
        pass

    def _create_blotter(self, cancel_policy=None):
        return simulation_blotter.Blotter(cancel_policy)





