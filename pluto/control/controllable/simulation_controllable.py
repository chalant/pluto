from pluto.finance.blotter import simulation_blotter
from pluto.control.controllable import controllable
from pluto import algorithm
from pluto.setup import setup as stp


class SimulationControllable(controllable.Controllable):
    def __init__(self):
        super(SimulationControllable, self).__init__()

    def _get_algorithm_class(self,
                             controllable,
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

    def _create_blotter(self, universe, cancel_policy):
        setup = stp.load_setup(universe)
        return simulation_blotter.SimulationBlotter(
            setup.get_commission_models(),
            setup.get_slippage_models(),
            cancel_policy)
