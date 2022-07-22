from pluto.control.controllable import controllable as cbl
from pluto.finance import live_blotter
from pluto import algorithm


class LiveControllable(cbl.Controllable):
    def _get_algorithm_class(self,
                             controllable,
                             pipeline_domain,
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
            pipeline_domain,
            initialize,
            handle_data,
            before_trading_start,
            analyze
        )

    def _update_blotter(self, blotter, broker_data):
        blotter.update(broker_data)

    def _create_blotter(self, session_id, engine_setup, cancel_policy):
        return live_blotter.LiveBlotter(
            session_id,
            self._broker,
            cancel_policy)

    def _update_account(self, blotter, main_account):
        blotter.update_account(main_account)
