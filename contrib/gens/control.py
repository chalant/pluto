import pandas as pd

from contextlib2 import ExitStack
from copy import copy
from logbook import Logger, Processor

from zipline.finance.order import ORDER_STATUS
from zipline.protocol import BarData
from zipline.utils.api_support import ZiplineAPI
from zipline.data import bundles
from zipline.data import data_portal as dp
from zipline.finance import metrics
from zipline.finance import trading
from zipline.finance import blotter
from zipline.pipeline import data
from zipline.pipeline import loaders

from zipline import algorithm

from contrib.finance.metrics import tracker
from contrib.sources import benchmark_source as bs
from contrib.control.clock import (
    BAR,
    SESSION_START,
    SESSION_END,
    MINUTE_END,
    BEFORE_TRADING_START_BAR,
    LIQUIDATE,
    STOP,
    INITIALIZE
)

log = Logger("ZiplineLog")

#todo: MetricsTracker and Account...

'''This is the "main loop" => controls the algorithm, and handles shut-down signals etc.'''
class AlgorithmController(object):
    def __init__(self, account, strategy, clock, benchmark_asset, restrictions, universe_func, bundler):
        self._bundler = bundler

        self._strategy = strategy
        self._account = account
        self._emission_rate = clock.emission_rate

        self._algo = None

        self._data_portal = None
        self._clock = clock

        self._run_dt = None

        #todo: need to validate the benchmark asset.
        self._benchmark_source = bs.BenchmarkSource(benchmark_asset)

        self._universe_func = universe_func

        self._restrictions = restrictions

        self._calendar = None

        self._current_data = None
        self._metrics_tracker = tracker.MetricsTracker(account, )

        # Processor function for injecting the algo_dt into
        # user prints/logs.
        def inject_algo_dt(record):
            if 'algo_dt' not in record.extra:
                record.extra['algo_dt'] = self.simulation_dt

        self._processor = Processor(inject_algo_dt)

        #we put some algorithm attributes here to extend there life
        self._capital_changes = {}
        self._capital_change_deltas = {}

        self._last_sync_time = pd.NaT

    def _get_run_dt(self):
        return self._run_dt

    @property
    def capital(self):
        return self._capital

    @capital.setter
    def capital(self, value):
        self._capital = value

    def run(self):
        def every_bar(dt_to_use, metrics_tracker, algorithm, handle_data_func, current_data):
            # syncs the account to the current bar...
            metrics_tracker.update(dt_to_use)

            # for capital_change in calculate_minute_capital_changes(
            #         dt_to_use, metrics_tracker.portfolio,
            #         emission_rate,data_portal, metrics_tracker):
            #     yield capital_change

            self._run_dt = dt_to_use
            algorithm.on_dt_changed(dt_to_use)
            handle_data_func(algorithm, current_data, dt_to_use)

            #this area is where new events occur from the handle_data_func (new orders etc.)

        def once_a_day(midnight_dt, algorithm, data_portal, metrics_tracker, trading_calendar):
            # for capital_change in self._calculate_capital_changes(
            #         midnight_dt, portfolio, emission_rate, True, data_portal, metrics_tracker):
            #     yield capital_change

            self._run_dt = midnight_dt
            algorithm.on_dt_changed(midnight_dt)

            metrics_tracker.handle_market_open(midnight_dt, data_portal, trading_calendar)

        def on_exit():
            # Remove references to algo, data portal, et al to break cycles
            # and ensure deterministic cleanup of these objects when the
            # simulation finishes.
            self._algo = None
            self.benchmark_source = self.current_data = self.data_portal = None

        with ExitStack() as stack:
            stack.callback(on_exit)
            stack.enter_context(self._processor)
            #stack.enter_context(ZiplineAPI(self._algo)) #todo
            #todo: the bundler has a data_frequency property?
            if self._bundler.data_frequency == 'minute':
                def execute_order_cancellation_policy(blotter, event):
                    blotter.execute_cancel_policy(event)

                def calculate_minute_capital_changes(dt, portfolio, emission_rate, dt_portal, mtr_tracker):
                    # process any capital changes that came between the last
                    # and current minutes
                    return self._calculate_capital_changes(
                        dt, portfolio, emission_rate, False, dt_portal, mtr_tracker)
            else:
                def execute_order_cancellation_policy():
                    pass

                def calculate_minute_capital_changes(dt):
                    return []

            for dt, action in self._clock:
                if action == INITIALIZE:
                    self._on_initialize(dt)

                elif action == BAR:
                    algo = self._algo
                    every_bar(dt, self._metrics_tracker, algo, algo.event_manager.handle_data, self._current_data)

                elif action == SESSION_START:
                    #re-initialize attributes
                    self._on_session_start(dt)
                    once_a_day(dt, self._algo, self._data_portal, self._metrics_tracker, self._clock.calendar)

                elif action == SESSION_END:
                    algo = self._algo
                    dp = self._data_portal
                    metrics_tracker = self._metrics_tracker
                    positions = metrics_tracker.positions
                    position_assets = self._asset_finder.retrieve_all(positions)
                    #todo: blotter?
                    #todo: is this step necessary? (clean_expired_assets?) For
                    self._cleanup_expired_assets(dt, self._blotter, position_assets, dp, metrics_tracker)

                    execute_order_cancellation_policy()
                    algo.validate_account_controls()

                    yield self._get_daily_message(dt, algo, metrics_tracker, dp)

                    self._on_session_end(dt)

                elif action == BEFORE_TRADING_START_BAR:
                    algo = self._algo
                    self._run_dt = dt
                    algo.on_dt_changed(dt)
                    algo.before_trading_start(self._current_data)

                elif action == MINUTE_END:
                    yield self._get_minute_message(
                        dt,
                        self._algo,
                        self._metrics_tracker,
                        self._data_portal
                    )

                    #if we have access to minute data, we should ingest data here.
                    if self._bundler.data_frequency == 'minute':
                        self._bundler.ingest()
                        self._load_attributes(dt,False)
                    #todo: perform a checkpoint in order to restore state from this point...
                    # (should be non-blocking)

                elif action == LIQUIDATE:
                    #todo
                    pass

                elif action == STOP:
                    #todo
                    pass
            # todo: the handle_simulation_end isn't necessary?
            risk_message = self._metrics_tracker.handle_simulation_end(
                self._data_portal
            )
            yield risk_message

    def _create_bar_data(self, universe_func, data_portal, get_simulation_dt, data_frequency, calendar, restrictions):
        return BarData(
            data_portal=data_portal,
            simulation_dt_func=get_simulation_dt,
            data_frequency=data_frequency,
            trading_calendar=calendar,
            restrictions=restrictions,
            universe_func=universe_func)

    def _get_daily_message(self, dt, algo, metrics_tracker, data_portal):
        """
        Get a perf message for the given datetime.
        """
        perf_message = metrics_tracker.handle_market_close(
            dt,
            data_portal,
        )
        perf_message['daily_perf']['recorded_vars'] = algo.recorded_vars
        return perf_message

    def _get_minute_message(self, dt, algo, metrics_tracker, data_portal):
        """
        Get a perf message for the given datetime.
        """
        rvars = algo.recorded_vars

        minute_message = metrics_tracker.handle_minute_close(
            dt,
            data_portal,
        )

        minute_message['minute_perf']['recorded_vars'] = rvars
        return minute_message

    def _cleanup_expired_assets(self, dt, blotter, position_assets, data_portal, metrics_tracker):
        """
        Clear out any assets that have expired before starting a new sim day.

        Performs two functions:

        1. Finds all assets for which we have open orders and clears any
           orders whose assets are on or after their auto_close_date.

        2. Finds all assets for which we have positions and generates
           close_position events for any assets that have reached their
           auto_close_date.
        """
        def past_auto_close_date(asset):
            acd = asset.auto_close_date
            return acd is not None and acd <= dt

        # Remove positions in any sids that have reached their auto_close date.
        assets_to_clear = \
            [asset for asset in position_assets if past_auto_close_date(asset)]
        for asset in assets_to_clear:
            metrics_tracker.process_close_position(asset, dt, data_portal)

        # Remove open orders for any sids that have reached their auto close
        # date. These orders get processed immediately because otherwise they
        # would not be processed until the first bar of the next day.
        assets_to_cancel = [
            asset for asset in blotter.open_orders
            if past_auto_close_date(asset)
        ]
        for asset in assets_to_cancel:
            blotter.cancel_all_orders_for_asset(asset)

        # Make a copy here so that we are not modifying the list that is being
        # iterated over.
        for order in copy(blotter.new_orders):
            if order.status == ORDER_STATUS.CANCELLED:
                metrics_tracker.process_order(order)
                blotter.new_orders.remove(order)

    def _load_data_portal(self, calendar, asset_finder,first_trading_day,
                          equity_minute_bar_reader,equity_daily_bar_reader,
                          adjustment_reader):
        return dp.DataPortal(
            asset_finder,
            trading_calendar=calendar,
            first_trading_day=first_trading_day,
            equity_minute_reader=equity_minute_bar_reader,
            equity_daily_reader=equity_daily_bar_reader,
            adjustment_reader=adjustment_reader
        )

    def _on_initialize(self, dt):
        # initialize all attributes
        self._load_attributes(dt,True)

    def _create_algorithm(self, start_session, end_session, capital_base,namespace,data_portal,
                          get_pipeline_loader, calendar, emission_rate, blotter, data_frequency='daily',
                          metrics_set='default', benchmark_returns=None):
        return algorithm.TradingAlgorithm(
            namespace={},
            data_portal=data_portal,
            get_pipeline_loader=get_pipeline_loader,
            trading_calendar=calendar,
            sim_params=trading.SimulationParameters(
                start_session=start_session,
                end_session=end_session,
                trading_calendar=calendar,
                capital_base=capital_base,
                data_frequency=data_frequency,
                emission_rate=emission_rate
            ),
            metrics_set=metrics.load(metrics_set),
            blotter=blotter,
            benchmark_returns=benchmark_returns,
            **{
                'initialize': self._strategy.initialize,
                'handle_data': self._strategy.handle_data,
                'before_trading_start': self._strategy.before_trading_start,
                'analyze': self._strategy.analyze
            }
        )

    def _on_session_start(self, dt):
        # initialize all attributes
        self._load_attributes(dt)

    def _load_attributes(self, dt, initialize=False):
        bundle = self._bundler.load()
        equity_minute_reader = bundle.equity_minute_bar_reader
        self._calendar = calendar = self._clock.calendar
        self._asset_finder = asset_finder = bundle.asset_finder
        self._data_portal = portal = self._load_data_portal(
            self._clock.calendar, asset_finder, equity_minute_reader.first_trading_day,
            equity_minute_reader, bundle.equity_daily_bar_reader, bundle.adjustment_reader
        )

        self._current_data = self._create_bar_data(
            self._universe_func,
            portal,
            self._get_run_dt,
            'daily',
            calendar,
            self._restrictions
        )

        pipeline_loader = loaders.USEquityPricingLoader(
            # use the current bundle's readers
            bundle.equity_daily_bar_reader,
            bundle.adjustment_reader
        )

        def choose_loader(column):
            if column in data.USEquityPricing.columns:
                return pipeline_loader
            raise ValueError(
                "No PipelineLoader registered for column %s." % column
            )

        self._algo = algo = self._create_algorithm(
            start_session=dt,
            calendar=self._clock.calendar,
            data_portal=self._data_portal,
            get_pipeline_loader=choose_loader,
            emission_rate=self._emission_rate)  # todo

        #initialize algorithm object
        algo.on_dt_changed(dt)
        if initialize:
            algo.initialize()

        #todo: handle_start_of_simulation (for metrics_tracker etc.)

    def _on_session_end(self, dt):
        self._bundler.ingest(timestamp=dt)

    def _calculate_capital_changes(self, dt, portfolio, emission_rate, is_interday,
                                  data_portal, metrics_tracker, portfolio_value_adjustment=0.0):
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
            capital_change = self._capital_changes[dt]
        except KeyError:
            return

        self._sync_last_sale_prices(data_portal, metrics_tracker, dt)
        if capital_change['type'] == 'target':
            target = capital_change['value']
            capital_change_amount = (
                target -
                (
                    portfolio.portfolio_value -
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

        self._capital_change_deltas.update({dt: capital_change_amount})
        metrics_tracker.capital_change(capital_change_amount)

        yield {
            'capital_change':
                {'date': dt,
                 'type': 'cash',
                 'target': target,
                 'delta': capital_change_amount}
        }

    def _sync_last_sale_prices(self, data_portal, metrics_tracker, dt):
        """Sync the last sale prices on the metrics tracker to a given
        datetime.

        Parameters
        ----------
        dt : datetime
            The time to sync the prices to.

        Notes
        -----
        This call is cached by the datetime. Repeated calls in the same bar
        are cheap.
        """
        if dt != self._last_sync_time:
            metrics_tracker.sync_last_sale_prices(
                dt,
                data_portal,
            )
            self._last_sync_time = dt

