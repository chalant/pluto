from pluto.control.controllable import controllable
from pluto.finance import live_blotter
from pluto import algorithm

class LiveControllable(controllable.Controllable):
    def __init__(self, broker_stub):
        super(LiveControllable, self).__init__()
        self._broker = broker_stub

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
        return algorithm.LiveTradingAlgorithm(
            controllable,
            params,
            blotter,
            metrics_tracker,
            pipeline_loader,
            initialize,
            before_trading_start,
            handle_data,
            analyze
        )

    def _update_blotter(self, blotter, broker_data):
        blotter.update(broker_data)

    def _create_blotter(self, universe, cancel_policy):
        return live_blotter.LiveBlotter(
            self._broker,
            cancel_policy)

    def _update_account(self, blotter, main_account):
        blotter.update_account(main_account)
