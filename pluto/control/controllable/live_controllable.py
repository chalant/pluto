from collections import deque

from pandas import DatetimeIndex

from pluto.control.controllable import controllable
from pluto.finance.blotter import liveblotter
from pluto import algorithm

class LiveControllable(controllable.Controllable):
    def __init__(self, broker_stub):
        super(LiveControllable, self).__init__()
        self._broker = broker_stub
        self._last_session_update = None

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
        return algorithm.LiveTradingAlgorithm(
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

    def _update_blotter(self, blotter, broker_data):
        blotter.update(broker_data)

    def _create_blotter(self, cancel_policy=None):
        return liveblotter.LiveBlotter(self._broker, cancel_policy)

    def _update_account(self, blotter, main_account):
        blotter.update_account(main_account)
