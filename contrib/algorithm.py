from zipline import TradingAlgorithm

from zipline.finance.metrics import tracker

from contrib.gens.control import AlgorithmController

class ContribTradingAlgorithm(TradingAlgorithm):
    def __init__(self,*args, **kwargs):
        super(ContribTradingAlgorithm, self).__init__(args, kwargs)
        self._account = kwargs.pop('account')

    def _create_generator(self, sim_params):
        return AlgorithmController(self, sim_params, )


    def _create_metrics_tracker(self):
        return tracker.MetricsTracker(
            trading_calendar=self.trading_calendar,
            first_session=self.sim_params.start_session,
            last_session=self.sim_params.end_session,
            capital_base=self.sim_params.capital_base,
            emission_rate=self.sim_params.emission_rate,
            data_frequency=self.sim_params.data_frequency,
            asset_finder=self.asset_finder,
            metrics=self._metrics_set,
            ledger=self._account
        )
