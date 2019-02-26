from contextlib2 import ExitStack
from copy import copy
from logbook import Logger, Processor

from zipline.finance.order import ORDER_STATUS
from zipline.protocol import BarData
from zipline.utils.api_support import ZiplineAPI
from zipline.data import bundles
from zipline.data import data_portal
from zipline.finance import metrics
from zipline.finance import trading
from zipline.finance import blotter
from zipline.pipeline import data
from zipline.pipeline import loaders

from zipline import algorithm

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

'''This is the "main loop" => controls the algorithm, and handles shut-down signals etc.'''
class AlgorithmController(object):
    def __init__(self, strategy, clock, benchmark_source, restrictions, universe_func, bundler):
        self._bundler = bundler
        self._bundle = None

        self._strategy = strategy
        self._account = None
        self._emission_rate = clock.emission_rate

        self._algo = None

        self._data_portal = None
        self._clock = clock

        self._run_dt = None

        self._benchmark_source = benchmark_source

        self._universe_func = universe_func

        self._restrictions = restrictions

        self._current_data = None

        self._load_attributes()

        # Processor function for injecting the algo_dt into
        # user prints/logs.
        def inject_algo_dt(record):
            if 'algo_dt' not in record.extra:
                record.extra['algo_dt'] = self.simulation_dt

        self._processor = Processor(inject_algo_dt)

    def _get_run_dt(self):
        return self._run_dt

    def run(self):
        algo = self._algo
        emission_rate = self._emission_rate
        account = self._account
        dp = self._data_portal

        current_data = self._current_data

        def every_bar(dt_to_use, current_data=current_data,
                      handle_data=algo.event_manager.handle_data):
            for capital_change in calculate_minute_capital_changes(dt_to_use):
                yield capital_change

            self._run_dt = dt_to_use
            algo.on_dt_changed(dt_to_use)
            #update the account class...
            account.update(dt_to_use)
            handle_data(algo, current_data, dt_to_use)

        def once_a_day(midnight_dt, current_data=current_data, data_portal=dp):
            for capital_change in algo.calculate_capital_changes(
                    midnight_dt,emission_rate=emission_rate,is_interday=True):
                yield capital_change

            self._run_dt = midnight_dt
            algo.on_dt_changed(midnight_dt)

            account.handle_market_open(midnight_dt, data_portal)

        def on_exit():
            # Remove references to algo, data portal, et al to break cycles
            # and ensure deterministic cleanup of these objects when the
            # simulation finishes.
            self.algo = None
            self.benchmark_source = self.current_data = self.data_portal = None

        with ExitStack() as stack:
            stack.callback(on_exit)
            stack.enter_context(self._processor)
            stack.enter_context(ZiplineAPI(algo))

            if algo.data_frequency == 'minute':
                def execute_order_cancellation_policy():
                    algo.blotter.execute_cancel_policy(SESSION_END)

                def calculate_minute_capital_changes(dt):
                    # process any capital changes that came between the last
                    # and current minutes
                    return algo.calculate_capital_changes(
                        dt, emission_rate=emission_rate, is_interday=False)
            else:
                def execute_order_cancellation_policy():
                    pass

                def calculate_minute_capital_changes(dt):
                    return []

            for dt, action in self._clock:
                if action == INITIALIZE:
                    #todo: initialization of everything (account, metrics_tracker etc.)
                    start_session = dt

                    pass
                if action == BAR:
                    for capital_change_packet in every_bar(dt):
                        yield capital_change_packet
                elif action == SESSION_START:
                    for capital_change_packet in once_a_day(dt):
                        yield capital_change_packet
                elif action == SESSION_END:
                    #todo: at each end of session, data ingestion is made, some things are re-loaded:
                    # data_portal etc.
                    positions = account.positions
                    position_assets = algo.asset_finder.retrieve_all(positions)
                    self._cleanup_expired_assets(dt, position_assets)

                    execute_order_cancellation_policy()
                    algo.validate_account_controls()

                    yield self._get_daily_message(dt, algo, account, dp)
                elif action == BEFORE_TRADING_START_BAR:
                    self.simulation_dt = dt
                    algo.on_dt_changed(dt)
                    algo.before_trading_start(current_data)
                elif action == MINUTE_END:
                    minute_msg = self._get_minute_message(
                        dt,
                        algo,
                        account,
                        dp
                    )
                    yield minute_msg
                elif action == LIQUIDATE:
                    #todo
                    pass
                elif action == STOP:
                    #todo
                    pass
            # todo: the handle_simulation_end isn't necessary
            risk_message = metrics_tracker.handle_simulation_end(
                dp,
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

    def _cleanup_expired_assets(self, dt, position_assets, data_portal):
        """
        Clear out any assets that have expired before starting a new sim day.

        Performs two functions:

        1. Finds all assets for which we have open orders and clears any
           orders whose assets are on or after their auto_close_date.

        2. Finds all assets for which we have positions and generates
           close_position events for any assets that have reached their
           auto_close_date.
        """
        algo = self._algo

        def past_auto_close_date(asset):
            acd = asset.auto_close_date
            return acd is not None and acd <= dt

        # Remove positions in any sids that have reached their auto_close date.
        assets_to_clear = \
            [asset for asset in position_assets if past_auto_close_date(asset)]
        metrics_tracker = algo.metrics_tracker
        for asset in assets_to_clear:
            metrics_tracker.process_close_position(asset, dt, data_portal)

        # Remove open orders for any sids that have reached their auto close
        # date. These orders get processed immediately because otherwise they
        # would not be processed until the first bar of the next day.
        blotter = algo.blotter
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
        return data_portal.DataPortal(
            asset_finder,
            trading_calendar=calendar,
            first_trading_day=first_trading_day,
            equity_minute_reader=equity_minute_bar_reader,
            equity_daily_reader=equity_daily_bar_reader,
            adjustment_reader=adjustment_reader
        )

    def _initialize(self, dt):
        # initialize all attributes
        self._load_attributes()

        def choose_loader(column):
            if column in data.USEquityPricing.columns:
                return self._pipeline_loader
            raise ValueError(
                "No PipelineLoader registered for column %s." % column
            )

        self._create_algorithm(
            start_session=dt,
            trading_calendar=self._clock.calendar,
            data_portal=self._data_portal,
            get_pipeline_loader=choose_loader,
            emission_rate=self._emission_rate) #todo

    def _create_algorithm(self,
                          start_session,
                          end_session,
                          trading_calendar,
                          capital_base,
                          namespace,
                          data_portal,
                          get_pipeline_loader,
                          calendar,
                          emission_rate,
                          blotter,
                          data_frequency='daily',
                          metrics_set='default',
                          benchmark_returns=None):

        self._algo = algorithm.TradingAlgorithm(
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

    def _load_attributes(self):
        # initialize all attributes
        self._bundle = bundle = self._bundler.load()
        equity_minute_reader = bundle.equity_minute_bar_reader
        calendar = self._clock.calendar
        self._data_portal = portal = self._load_data_portal(
            self._clock.calendar, bundle.asset_finder, equity_minute_reader.first_trading_day,
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

        self._pipeline_loader =  loaders.USEquityPricingLoader(
            # use the current bundle's readers
            bundle.equity_daily_bar_reader,
            bundle.adjustment_reader
        )

    def _on_session_start(self, dt):
        # re-load bundle and data_portal
        self._load_attributes()

    def _on_session_end(self, dt):
        self._bundler.ingest(timestamp=dt)

