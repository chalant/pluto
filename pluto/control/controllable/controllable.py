from abc import ABC, abstractmethod
from copy import copy

import pandas as pd
import logbook

from zipline.finance import trading
from zipline.data import data_portal as dp
from zipline.protocol import BarData
from zipline.finance import asset_restrictions
from zipline.finance.order import ORDER_STATUS
from zipline.pipeline.data import USEquityPricing
from zipline.pipeline.loaders import USEquityPricingLoader

from pluto.finance.metrics import tracker
from pluto.sources import benchmark_source as bs
from pluto.utils import saving
from pluto.data import bundler

log = logbook.Logger('Controllable')

class Controllable(ABC, saving.Savable):
    def __init__(self):
        self._metrics_tracker = None
        self._bundle = None
        self._calendar = None

        self._blotter = None
        self._asset_finder = None
        self._algo = None

        self._account = None
        self._current_data = None

        self._calculate_minute_capital_changes = None
        self._emission_rate = 'daily'

        self._capital_change_deltas = {}
        self._capital_changes = {}

        # script namespace
        self._namespace = {}

        self._calendar = None
        self._universe = None

        self._sessions = pd.Series()

        self._end_dt = None
        self._look_back = None

    def initialize(self,
                   start_dt,
                   end_dt,
                   universe,
                   strategy,
                   bundle_name,
                   capital,
                   max_leverage,
                   data_frequency,
                   arena,
                   platform,
                   look_back):

        calendar = universe.get_calendar(start_dt, end_dt)
        # end_dt is the previous day
        self._end_dt = end_dt
        self._calendar = calendar
        self._universe = universe
        self._look_back = look_back
        if data_frequency == 'minute':
            self._emission_rate = 'minute'

            def calculate_minute_capital_changes(dt, algo, emission_rate):
                self._calculate_capital_changes(dt, emission_rate, is_interday=False)
        else:
            def calculate_minute_capital_changes(dt, algo, emission_rate):
                return []

        self._calculate_minute_capital_changes = calculate_minute_capital_changes

        self._params = params = trading.SimulationParameters(
            start_dt,
            end_dt,
            calendar,
            capital,
            data_frequency=data_frequency,
            arena=arena)

        self._sessions = sessions = params.sessions

        # we assume that the data has already been ingested => the controller must first
        # send data. An error is raised if there is no data
        loader = bundler.get_bundle_loader()  # todo
        bundle = loader(bundle_name)
        self._asset_finder = asset_finder = bundle.asset_finder
        first_trading_day = bundle.equity_daily_bar_reader.first_trading_day

        self._data_portal = data_portal = dp.DataPortal(
            asset_finder=asset_finder,
            trading_calendar=calendar,
            first_trading_day=first_trading_day,
            equity_minute_reader=bundle.equity_minute_bar_reader,
            equity_daily_reader=bundle.equity_daily_bar_reader,
            adjustment_reader=bundle.adjustment_reader
        )

        # load s&p 500 index as benchmark_asset.
        # todo: we can't load the benchmark data like this for now...
        # todo: we need a special file for benchmark data...
        asset = asset_finder.lookup_symbol('^GSPC', end_dt)
        self._benchmark_source = benchmark_source = bs.BenchmarkSource(
            asset,
            calendar,
            sessions,
            self._data_portal,
            self._emission_rate)

        self._metrics_tracker = metrics_tracker = \
            tracker.MetricsTracker(
                benchmark_source,
                capital,
                data_frequency,
                start_dt,
                look_back)
        self._blotter = blotter = self._create_blotter()

        restrictions = asset_restrictions.NoRestrictions()

        self._current_data = self._create_bar_data(
            data_portal,
            calendar,
            restrictions,
            data_frequency)

        # todo: we need more pipeline loaders (fundamentals etc.)
        pipeline_loader = USEquityPricingLoader.without_fx(
            bundle.equity_daily_bar_reader,
            bundle.adjustment_reader,
        )

        def choose_loader(column):
            if column in USEquityPricing.columns:
                return pipeline_loader
            raise ValueError(
                "No PipelineLoader registered for column %s." % column
            )

        def noop(*args, **kwargs):
            pass

        code = compile(strategy, '', 'exec')
        exec(code, self._namespace)
        namespace = self._namespace

        # the algorithm object is just for exposing methods (api) that are needed by the user
        # (we don't run the algorithm through the algorithm object)

        self._algo = self._get_algorithm_class(
            params,
            data_portal,
            blotter,
            metrics_tracker,
            choose_loader,
            namespace.get('initialize', noop),
            namespace.get('before_trading_start', noop),
            namespace.get('handle_data', noop),
            namespace.get('analyze', noop))

    @abstractmethod
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
        raise NotImplementedError(self._get_algorithm_class.__name__)

    def minute_end(self, dt):
        return self._get_minute_message(dt, self._algo, self._metrics_tracker, self._data_portal)

    def session_start(self, dt):

        end_dt = self._end_dt
        if dt == end_dt:
            #reload a new calendar
            start_dt = end_dt - pd.Timedelta(days=self._look_back)
            self._calendar = self._universe.get_calendar(start_dt, end_dt)

        self._sync_last_sale_prices(dt)
        self._calculate_capital_changes(dt, self._emission_rate, is_interday=False)

        algo = self._algo
        metrics_tracker = self._metrics_tracker
        data_portal = self._data_portal
        self._current_dt = dt

        algo.on_dt_changed(dt)
        metrics_tracker.handle_market_open(
            dt,
            data_portal)

        # handle any splits that impact any positions or any open orders.
        assets_we_care_about = (
                metrics_tracker.positions.keys() |
                algo.blotter.open_orders.keys()
        )

        if assets_we_care_about:
            splits = data_portal.get_splits(assets_we_care_about, dt)
            if splits:
                algo.blotter.process_splits(splits)
                metrics_tracker.handle_splits(splits)

    def before_trading_starts(self, dt):
        algo = self._algo
        self._current_dt = dt
        algo.on_dt_changed(dt)
        algo.before_trading_start(self._current_data)

    def bar(self, dt):
        self._sync_last_sale_prices(dt)
        self._current_dt = dt

        algo = self._algo
        algo.on_dt_changed(dt)
        blotter = self._blotter
        metrics_tracker = self._metrics_tracker
        calendar = self._calendar
        sessions = self._sessions

        capital_changes = self._calculate_minute_capital_changes(dt, self._emission_rate)

        # todo: assets are must be restricted to the provided exchanges

        # self._restrictions.set_exchanges(exchanges)
        current_data = self._current_data

        new_transactions, new_commissions, closed_orders = blotter.get_transactions(current_data)

        for transaction in new_transactions:
            metrics_tracker.process_transaction(transaction)

            order = blotter.orders[transaction.order_id]
            metrics_tracker.process_order(order)

        for commission in new_commissions:
            metrics_tracker.process_commission(commission)

        algo.event_manager.handle_data(algo, current_data, dt)

        # grab any new orders from the blotter, then clear the list.
        # this includes cancelled orders.
        new_orders = blotter.new_orders
        blotter.new_orders = []

        # if we have any new orders, record them so that we know
        # in what perf period they were placed.
        for new_order in new_orders:
            metrics_tracker.process_order(new_order)

        portal = self._data_portal
        self._benchmark_source.on_minute_end(dt, portal, calendar, sessions)
        metrics_tracker.handle_minute_close(dt, portal, sessions)

        # todo: save state in some file (also add a controllable state?)
        # todo: this should be handled by a thread. Note: this must be done at the end of a bar
        # todo: when restoring state, we need to process orders that happened between last_checkpoint
        #  and today
        state = metrics_tracker.get_state(dt)

        return capital_changes

    def session_end(self, dt):
        metrics_tracker = self._metrics_tracker

        positions = metrics_tracker.positions
        position_assets = self._asset_finder.retrieve_all(positions)
        blotter = self._blotter
        data_portal = self._data_portal
        calendar = self._calendar
        algo = self._algo

        self._cleanup_expired_assets(dt, data_portal, blotter, metrics_tracker, position_assets)

        algo.validate_algo_controls()

        # updates the sessions (live)
        self._sessions = sessions = self._get_sessions(dt, self._params)
        return self._get_daily_message(
            dt,
            algo,
            metrics_tracker,
            data_portal,
            calendar,
            sessions
        )

    def _get_sessions(self, dt, params):
        sessions = self._sessions_array
        if sessions[-1] != dt:
            sessions.popleft()
            sessions.append(dt)
            params.start_session = sessions[0].normalize()
            params.end_session = dt
            return pd.DatetimeIndex(sessions)
        else:
            return self._sessions

    def stop(self, dt):
        # todo: sell all the positions
        pass

    def liquidate(self, dt):
        pass

    def update_blotter(self, broker_data):
        self._update_blotter(self._blotter, broker_data)

    @abstractmethod
    def _update_blotter(self, blotter, broker_data):
        raise NotImplementedError

    def update_account(self, main_account):
        self._update_account(self._blotter, main_account)

    @abstractmethod
    def _update_account(self, blotter, main_account):
        raise NotImplementedError

    def update_capital(self, dt, capital):
        self._capital_changes = {dt: {'type': 'target', 'value': capital}}

    def update_calendar(self, dt, calendar):
        raise NotImplementedError(self.update_calendar.__name__)

    def get_simulation_dt(self):
        return self._current_dt

    @abstractmethod
    def _create_blotter(self, cancel_policy=None):
        raise NotImplementedError(self._create_blotter.__name__)

    def _get_daily_message(self, dt, algo, metrics_tracker, data_portal, trading_calendar, sessions):
        '''

        Parameters
        ----------
        dt
        algo: pluto.algorithm.TradingAlgorithm
        metrics_tracker: pluto.finance.metrics.tracker.MetricsTracker
        data_portal: zipline.data.data_portal.DataPortal
        trading_calendar: trading_calendars.TradingCalendar

        Returns
        -------

        '''
        """
        Get a perf message for the given datetime.
        """
        perf_message = metrics_tracker.handle_market_close(
            dt,
            data_portal,
            trading_calendar,
            sessions
        )
        perf_message['daily_perf']['recorded_vars'] = algo.recorded_vars
        return perf_message

    def _get_minute_message(self, dt, algo, metrics_tracker, data_portal, trading_calendar, sessions):
        '''

        Parameters
        ----------
        dt
        algo
        metrics_tracker: pluto.finance.metrics.tracker.MetricsTracker
        data_portal
        trading_calendar: trading_calendars.TradingCalendar

        Returns
        -------

        '''
        """
        Get a perf message for the given datetime.
        """
        rvars = algo.recorded_vars

        minute_message = metrics_tracker.handle_minute_close(
            dt,
            data_portal,
            trading_calendar,
            sessions
        )

        minute_message['minute_perf']['recorded_vars'] = rvars
        return minute_message

    def _create_bar_data(self, data_portal, calendar, restrictions, data_frequency):
        return BarData(
            data_portal=data_portal,
            simulation_dt_func=self.get_simulation_dt,
            data_frequency=data_frequency,
            trading_calendar=calendar,
            restrictions=restrictions
        )

    def _cleanup_expired_assets(self, dt, data_portal, blotter, metrics_tracker, position_assets):
        '''

        Parameters
        ----------
        dt
        data_portal
        blotter
        metrics_tracker: pluto.finance.metrics.tracker.MetricsTracker
        position_assets

        Returns
        -------

        '''
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

    def _sync_last_sale_prices(self, dt=None):
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
            self._metrics_tracker.sync_last_sale_prices(
                dt,
                self._data_portal,
            )
            self._last_sync_time = dt

    def _calculate_capital_changes(self, dt, emission_rate, is_interday,
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
            capital_change = self._capital_changes[dt]
        except KeyError:
            return

        if capital_change['type'] == 'target':
            target = capital_change['value']
            capital_change_amount = (
                    target -
                    (
                            self._metrics_tracker.portfolio.portfolio_value -
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
        self._metrics_tracker.capital_change(capital_change_amount)

        yield {
            'capital_change':
                {'date': dt,
                 'type': 'cash',
                 'target': target,
                 'delta': capital_change_amount}
        }
