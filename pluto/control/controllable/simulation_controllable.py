from pluto.finance.blotter import simulation_blotter
from pluto.control.controllable import controllable as cbl
from pluto import algorithm


class SimulationControllable(cbl.Controllable):
    def __init__(self):
        super(SimulationControllable, self).__init__()

    def _get_algorithm_class(self,
                             controllable,
                             universe,
                             params,
                             blotter,
                             metrics_tracker,
                             pipeline_loader,
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
            controllable,
            universe,
            params,
            blotter,
            metrics_tracker,
            pipeline_loader,
            initialize,
            handle_data,
            before_trading_start,
            analyze
        )

    def _update_blotter(self, blotter, broker_data):
        pass

    def _update_account(self, blotter, main_account):
        pass

    def _create_blotter(self, session_id, engine_setup, cancel_policy):

        return simulation_blotter.SimulationBlotter(
            engine_setup,
            cancel_policy)

    def restore_state(self, state, strategy):
        pass