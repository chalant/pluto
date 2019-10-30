from zipline.finance.metrics import tracker

from contrib.control.controllable.control import AlgorithmController

class ContribTradingAlgorithm(object):
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

    def calculate_capital_changes(self, dt, emission_rate, is_interday,
                                  portfolio_value_adjustment=0.0):
        """
        If there is a capital change for a given dt, this means that the change
        occurs before `handle_data` on the given dt. In the case of the
        change being a target value, the change will be computed on the
        portfolio value according to prices at the given dt

        `portfolio_value_adjustment`, if specified, will be removed from the
        portfolio_value of the cumulative performance when calculating deltas
        from target capital changes.
        """
        try:
            capital_change = self.capital_changes[dt]
        except KeyError:
            return

        self._sync_last_sale_prices()
        if capital_change['type'] == 'target':
            target = capital_change['value']
            capital_change_amount = (
                target -
                (
                    self.portfolio.portfolio_value -
                    portfolio_value_adjustment
                )
            )

            log.info('Processing capital change to target %s at %s. Capital '
                     'change delta is %s' % (target, dt,
                                             capital_change_amount))
        elif capital_change['type'] == 'delta':
            target = None
            capital_change_amount = capital_change['value']
            log.info('Processing capital change of delta %s at %s'
                     % (capital_change_amount, dt))
        else:
            log.error("Capital change %s does not indicate a valid type "
                      "('target' or 'delta')" % capital_change)
            return

        self.capital_change_deltas.update({dt: capital_change_amount})
        self.metrics_tracker.capital_change(capital_change_amount)

        yield {
            'capital_change':
                {'date': dt,
                 'type': 'cash',
                 'target': target,
                 'delta': capital_change_amount}
        }
