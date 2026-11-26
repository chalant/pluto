from abc import ABC, abstractmethod
from copy import copy
import numpy as np
from collections import deque

import pandas as pd

import logbook

from zipline.finance import asset_restrictions, trading
from zipline.gens.sim_engine import SESSION_END
from zipline.finance.order import ORDER_STATUS

from zipline.data import data_portal as dp

from zipline import protocol
from zipline.protocol import BarData

from zipline.utils import api_support

from pluto.pipeline import domain
from pluto.coms.utils import conversions

from pluto.finance import ledger, position_tracker
from pluto.finance.metrics import tracker
from pluto.finance.broker import broker_setup as brs

from pluto.data.universes import core

from pluto.control.clock import clock

from protos import controllable_pb2

from dev import events

log = logbook.Logger('Controllable')


# todo: trigger the order placed event here...
class _State(ABC):
    @abstractmethod
    def handle_data(self, algo, current_data, dt):
        raise NotImplementedError

    @abstractmethod
    def before_trading_starts(self, algo, dt):
        raise NotImplementedError

    def send_orders(self, blotter, dt):
        self._send_orders(blotter, dt)

    @abstractmethod
    def _send_orders(self, orders, dt):
        raise NotImplementedError


class _Recovering(_State):
    def handle_data(self, algo, current_data, dt):
        # does nothing
        pass

    def before_trading_starts(self, algo, dt):
        pass

    def _send_orders(self, orders, dt):
        pass


# This state is made to make sure that functions are "activated in the right order
class _Transitioning(_State):

    def __init__(self, controllable):
        '''

        Parameters
        ----------
        controllable: Controllable
        '''
        self._bfs_flag = False
        self._controllable = controllable

    def handle_data(self, algo, current_data, dt):
        if self._bfs_flag:
            controllable = self._controllable
            controllable.run_state = controllable.ready
            algo.event_manager.handle_data(algo, current_data, dt)

    def before_trading_starts(self, algo, dt):
        algo.before_trading_start(algo, dt)
        self._bfs_flag = True

    def _send_orders(self, orders, dt):
        if self._bfs_flag:
            # todo: send orders to broker...
            pass


class _Ready(_State):
    def __init__(self, broker):
        self._events = {}
        self._orders = {}

        self._broker = broker

    def handle_data(self, algo, current_data, dt):
        algo.event_manager.handle_data(algo, current_data, dt)

    def before_trading_starts(self, algo, current_dt):
        algo.before_trading_start(algo, current_dt)

    def _send_orders(self, blotter, dt):
        pass


class Controllable(ABC):
    def __init__(self):
        self._metrics_tracker = None

        self._data_portal = None
        self._blotter = None
        self._asset_finder = None
        self._algo = None

        self._account = None
        self._current_data = None

        self._last_sync_time = None
        self._calculate_minute_capital_changes = None

        self._emission_rate = 'daily'
        self._data_frequency = 'daily'

        self._broker_setup = brs.BrokerSetup()

        self._capital_change_deltas = {}
        self._capital_changes = {}

        # script namespace
        self._namespace = {}

        self._positions_tracker = None
        self._calendar = None
        self._universe = None
        self._benchmark = None
        # self._benchmark_source = None
        self._last_session_close = None

        self._sessions = pd.Series(dtype='float64')
        self._sessions_array = deque()

        self._name = None
        self._session_id = None
        self._start_dt = None
        self._end_dt = None
        self._look_back = None
        self._current_dt = None
        self._real_dt = None
        self._params = None

        self._broker = None
        self._ready = _Ready(self._broker)
        self._transitioning = _Transitioning(self)
        self._recovering = recovering = _Recovering()
        self._run_state = recovering

        self._clock = clock.ExplodingClock()

        self._sync_state_tracker = None

        self._restrictions = asset_restrictions.NoRestrictions()

        self._mode = None
        self.flag = False

        self._not_running = 0
        self._ended = False

        self._transactions = []

    @property
    def not_running(self):
        return self._not_running

    @not_running.setter
    def not_running(self, value):
        self._not_running = value

    @property
    def ended(self):
        return self._ended

    @ended.setter
    def ended(self, value):
        self._ended = value

    @property
    def broker(self):
        return self._broker

    @property
    def datetime(self):
        return self._current_dt

    @property
    def data_frequency(self):
        return self._data_frequency

    @property
    def session_id(self):
        return self._session_id

    @property
    def clock(self):
        return self._clock

    @property
    def state(self):
        return self._sync_state_tracker

    @property
    def current_dt(self):
        return self._current_dt

    @property
    def real_dt(self):
        return self._real_dt

    @real_dt.setter
    def real_dt(self, dt):
        self._real_dt = dt

    @property
    def run_state(self):
        return self._run_state

    @run_state.setter
    def run_state(self, value):
        self._run_state = value

    @property
    def recovering(self):
        return self._recovering

    @property
    def transitioning(self):
        return self._transitioning

    @property
    def ready(self):
        return self._ready

    @property
    def data_portal(self):
        return self._data_portal

    @property
    def trading_calendar(self):
        return self._calendar

    @property
    def asset_finder(self):
        return self._asset_finder

    @property
    def run_params(self):
        return self._params

    @property
    def current_data(self):
        return self._current_data

    def set_broker(self, broker):
        self._broker = broker

    def initialize(self,
                   strategy_setup,
                   strategy,
                   arena):
        """

        Parameters
        ----------
        strategy_setup: pluto.strategies.setup.StrategySetup
        strategy: bytes
        arena: str
        """

        def noop(*args, **kwargs):
            pass

        namespace = self._namespace

        code = compile(strategy, '', 'exec')
        exec(code, namespace)

        # execute setup function first
        namespace.get('requirement', noop)

        # load parameters from database
        self._look_back = strategy_setup.look_back
        self._mode = arena

    @property
    def strategy_name(self):
        return self._name

    def run(self,
            broker,
            process_id,
            directory,
            strategy_setup,
            broker_setup,
            run_setup,
            data_cache,
            environ,
            start_dt,
            end_dt):

        """

        Parameters
        ----------
        broker: pluto.finance.broker.broker_stub.BrokerStub
        process_id: str
        directory: directory.utils.DirectoryInterface
        strategy_setup: pluto.strategies.setup.StrategySetup
        broker_setup: pluto.broker.broker_setup.BrokerSetup
        run_setup
        data_cache: pluto.data.caching.Cache
        environ: dict
        start_dt: pandas.Timestamp
        end_dt: pandas.Timestamp

        Returns
        -------

        """

        def noop(*args, **kwargs):
            pass

        self._broker = broker
        self._session_id = process_id
        self._broker_setup = broker_setup

        namespace = self._namespace
        # look_back = self._look_back

        data_frequency = strategy_setup.data_frequency

        self._name = strategy_setup.strategy_id

        uni = core.get_universe(
            run_setup.trading_universe,
            directory,
            data_cache,
            environ
        )

        self._clock = cl = clock.Clock(
            uni.calendar_name,
            start_dt,
            end_dt)

        events.subscribe(
            'clock.calendar_update',
            self._update_calendar)

        # self._sync_state_tracker = sst = ss.Tracker(uni.calendars_names)

        # calendar = uni.load_calendar(
        #     start_dt - pd.Timedelta(days=look_back),
        #     end_dt)

        calendar = cl.calendar

        self._start_dt = start_dt

        end_dt = calendar.last_session

        # load parameters from database

        self._end_dt = end_dt
        self._last_session_close = calendar.session_close(end_dt)
        self._calendar = calendar
        self._universe = uni
        self._look_back = strategy_setup.look_back
        self._data_frequency = strategy_setup.data_frequency

        if strategy_setup.data_frequency == 'minute':
            # always set the emission_rate to a minute if the data_frequency is a minute
            self._emission_rate = 'minute'

            def calculate_minute_capital_changes(dt, metrics_tracker, emission_rate):
                self._calculate_capital_changes(
                    dt,
                    metrics_tracker,
                    emission_rate,
                    is_interday=False)
        else:
            def calculate_minute_capital_changes(dt, metrics_tracker, emission_rate):
                return []

        self._calculate_minute_capital_changes = calculate_minute_capital_changes

        # todo: this could be loaded from files and account parameters (capital etc.)
        self._params = params = trading.SimulationParameters(
            # sessions are time-zone naive (exchange_calendars)
            start_dt.tz_localize(None) if start_dt.tzinfo is not None else start_dt,
            end_dt,
            calendar,
            run_setup.capital,
            emission_rate=data_frequency,
            data_frequency=data_frequency,
            arena=self._mode)

        self._sessions = sessions = params.sessions

        self._sessions_array = deque(sessions)

        # we assume that the data has already been ingested => the controller must first
        # send data. An error is raised if there is no data

        bundle = uni

        self._asset_finder = asset_finder = uni.asset_finder

        last_session = calendar.last_session

        # todo: we should raise an error if the there is no data for the look back
        #  period...

        self._data_portal = data_portal = dp.DataPortal(
            asset_finder=asset_finder,
            trading_calendar=calendar,
            first_trading_day=max(
                bundle.first_trading_day,
                calendar.sessions[0]),
            equity_minute_reader=bundle.equity_minute_bar_reader,
            equity_daily_reader=bundle.equity_daily_bar_reader,
            adjustment_reader=bundle.adjustment_reader,
            last_available_session=last_session,
            last_available_minute=calendar.session_last_minute(last_session))

        # self._benchmark_source = benchmark_source = bs.SimulationBenchmarkSource(
        #     calendar,
        #     sessions,
        #     uni.benchmark,
        #     self._look_back,
        #     self._emission_rate)

        self._positions_tracker = pos_tracker = position_tracker.PositionTracker(
            protocol.Positions()
        )

        # todo: metrics tracker should not take a benchmark source
        self._metrics_tracker = metrics_tracker = tracker.MetricsTracker(
            ledger.Ledger(
                start_dt,
                protocol.Portfolio(start_dt, run_setup.capital),
                protocol.Account(),
                pos_tracker),
            run_setup.capital,
            data_frequency,
            self._emission_rate,
            start_dt)

        # todo: we should set up the cancel policy externally.

        self._blotter = blotter = self._create_blotter(
            process_id,
            run_setup,
            strategy_setup.cancel_policy)

        self._current_data = self._create_bar_data(
            data_portal,
            calendar,
            self._restrictions,
            data_frequency)

        dom = domain.Domain(
            uni.country_code,
            calendar)

        # todo: pipeline loader should be created from a script...

        # the algorithm object is just for exposing methods (api) that are needed by the user
        # (we don't run the algorithm through the algorithm object)

        self._algo = algo = self._get_algorithm_class(
            controllable=self,
            params=params,
            blotter=blotter,
            metrics_tracker=metrics_tracker,
            pipeline_loader=uni.pipeline_loader(dom),
            pipeline_domain=dom,
            initialize=namespace.get('initialize', noop),
            handle_data=namespace.get('handle_data', noop),
            before_trading_start=namespace.get('before_trading_start', noop),
            analyze=noop)

        # sst.state = sst.out_session
        self._run_state = self._ready
        algo.set_pipeline_domain(dom)
        # schedule_function binds its rules to this calendar (week_start,
        # month_start... need one); it was only set on a live calendar update
        algo.set_trading_calendar(calendar)

        api_support.set_algo_instance(algo)
        algo.on_dt_changed(start_dt)
        # initialize the algo (called only once per lifetime)
        algo.initialize(**{})  # todo: kwargs?
        algo.initialized = True

    @property
    def broker_setup(self):
        return self._broker_setup

    @abstractmethod
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
        """
        Returns
        -------
        pluto.algorithm.TradingAlgorithm
        """

        raise NotImplementedError(self._get_algorithm_class.__name__)

    def minute_end(self, dt):
        """Returns whether the minute was the last of the run."""
        # revalued here, as the perf packet of the end of the minute did when it read the portfolio (see session_end)
        self._metrics_tracker.ledger.revalue()

        return dt == self._end_dt

    def session_start(self, dt):
        # if dt > self._end_dt:
        #     self._last_session_close = self._calendar.session_close(dt)

        # end_dt = self._end_dt
        #
        # if dt > end_dt:
        #     # todo: we need a better way for reloading everything
        #     #  we should only reload the calendar, since the data gets
        #     #  updated in the background...
        #
        #     # reload a new calendar
        #     start_dt = dt - pd.Timedelta(days=self._look_back)
        #     self._end_dt = dt
        #
        #     # todo: reload equity minute bar reader etc (reload bundle)
        #
        #     self._calendar = calendar = self._universe.load_calendar(
        #         start_dt,
        #         dt)
        #
        #     self._last_session_close = calendar.session_close(dt)
        #
        #     # fixme: this might not be correct
        #     self._start_dt = fs = calendar.first_session
        #     look_back = calendar.last_session - fs
        #
        #     sessions = self._sessions_array
        #     # reload calendar if it is not the full look-back period
        #     if look_back < self._look_back:
        #         self._end_dt = end = fs + pd.Timedelta(days=150)
        #         self._calendar = calendar = self._universe.load_calendar(fs, end)
        #         dt = end
        #
        #     sessions.popleft()
        #     sessions.append(dt)
        #
        #     # updates the sessions (live)
        #     params = self._params
        #     self._params = trading.SimulationParameters(
        #         fs,
        #         dt,
        #         calendar,
        #         params.capital_base,
        #         params.emission_rate,
        #         params.data_frequency,
        #         params.arena
        #     )
        #
        #     # reload bundle so that it updates the calendar instance
        #     bundle = self._universe
        #
        #     self._data_portal = data_portal = dp.DataPortal(
        #         asset_finder=self._asset_finder,
        #         trading_calendar=calendar,
        #         first_trading_day=fs,
        #         equity_minute_reader=bundle.equity_minute_bar_reader,
        #         equity_daily_reader=bundle.equity_daily_bar_reader,
        #         adjustment_reader=bundle.adjustment_reader
        #     )
        #
        #     self._current_data = self._create_bar_data(
        #         data_portal,
        #         calendar,
        #         self._restrictions,
        #         params.data_frequency)

        # clear previous transactions
        self._transactions.clear()

        metrics_tracker = self._metrics_tracker

        capital_changes = self._calculate_capital_changes(
            dt,
            metrics_tracker,
            self._emission_rate,
            is_interday=False)

        algo = self._algo

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

        return capital_changes

    def before_trading_starts(self, dt):
        algo = self._algo
        self._current_dt = dt
        algo.on_dt_changed(dt)

        api_support.set_algo_instance(algo)
        self._run_state.before_trading_starts(algo, self._current_data)

    def bar(self, dt):
        metrics_tracker = self._metrics_tracker
        self._current_dt = dt

        algo = self._algo
        algo.on_dt_changed(dt)
        api_support.set_algo_instance(algo)
        blotter = self._blotter

        # capital_changes = self._calculate_minute_capital_changes(
        #     dt,
        #     metrics_tracker,
        #     self._emission_rate)

        current_data = self._current_data

        # this is where we update everything (ledger etc.)
        closed_orders = []
        commissions = []

        for order, transaction, commission in self._transactions:
            order.filled += transaction.amount
            order.commission += commission

            order.dt = transaction.dt

            if not order.open:
                closed_orders.append(order)

            metrics_tracker.process_transaction(transaction)

            metrics_tracker.process_order(order)

            if commission > 0:
                commissions.append(
                    {
                        'asset': order.asset,
                        'order': order,
                        'cost': commission
                    }
                )

        # todo: add tax handling here!

        # all the transactions first, then all the commissions, as zipline's
        # AlgorithmSimulator (the order changes the floating-point cash)
        for commission in commissions:
            metrics_tracker.process_commission(commission)

        blotter.prune_orders(closed_orders)

        # handle_data is not called while in recovery
        self._run_state.handle_data(algo, current_data, dt)
        # self._sync_last_sale_prices(metrics_tracker, dt)

        # grab any new orders from the blotter, then clear the list.
        # this includes cancelled orders.

        # if we have any new orders, record them so that we know
        # in what perf period they were placed.
        for new_order in blotter.new_orders:
            metrics_tracker.process_order(new_order)

        return blotter

    def update_blotter_and_positions(self, broker_data):
        # self._update_blotter(self._blotter, broker_data)
        ts = broker_data.timestamp
        transactions = self._transactions

        orders = self._blotter.orders

        for ct in broker_data.transactions:
            order_id = ct.transaction.order_id
            order_ = orders.get(order_id, None)

            if order_:
                transactions.append((
                    order_,
                    conversions.to_zp_transaction(ct.transaction),
                    ct.commission))

        pos_tracker = self._positions_tracker
        # make a copy of the previous positions

        for asset_price in broker_data.last_sale_prices:
            sid = asset_price.sid
            price = asset_price.price

            if not np.isnan(price):
                pos = pos_tracker.positions[sid].inner_position
                pos.last_sale_price = price
                pos.last_sale_date = conversions.to_pandas_timestamp(ts)

        self._metrics_tracker.ledger._dirty_portfolio = True
        self._positions_tracker._dirty_stats = True

        # for commission in broker_data.commissions:
        #     order = conversions.to_zp_order(commisupdate_sion.order)
        #     order_id = order.id
        #     o = orders.get(order_id, None)
        #     if o:
        #         commissions.append({
        #             'asset': conversions.to_zp_asset(commission.asset),
        #             'cost': commission.cost,
        #             'order': order
        #         })
        #
        #     orders[order_id] = order  # update the order state

    def session_end(self, dt):
        """Ends the session: expired assets, the cancel policy, the account controls. Returns whether it was the
        last session of the run."""
        metrics_tracker = self._metrics_tracker

        positions = metrics_tracker.positions
        position_assets = self._asset_finder.retrieve_all(positions)
        blotter = self._blotter
        data_portal = self._data_portal
        algo = self._algo

        # fixme: this must be handled by the controller.
        self._cleanup_expired_assets(
            dt,
            data_portal,
            blotter,
            metrics_tracker,
            position_assets)

        # fixme: validate account controls controller side.

        # mirrors zipline-reloaded 698b7dbb (ENH: enabled EODCancel for daily
        # frequency): at the end of a daily session the oldest of the open
        # orders of an asset that has several is cancelled; the cancellation is
        # relayed (to the controller) with the orders of the next bar.
        # todo: the cancel policy of minute runs (blotter.execute_cancel_policy)
        #  needs the controller to cancel its copies of the orders: it is not
        #  executed (it never was on this path).
        if self._data_frequency == 'daily':
            blotter.execute_daily_cancel_policy(SESSION_END)
        algo.validate_account_controls()

        # the runtime ledger is dirty after every account update, and what the strategy reads of its portfolio
        # (returns and pnl compound at each revaluation) depends on where it is revalued: here, as the perf
        # packet of the end of the session did when it read the portfolio (zipline's handle_market_close does too)
        metrics_tracker.ledger.revalue()

        return dt == self._last_session_close

    def get_state(self, dt):
        metrics_tracker = self._metrics_tracker
        return controllable_pb2.ControllableState(
            session_id=self._session_id,
            session_state=self._sync_state_tracker.state.name,
            capital=metrics_tracker.portfolio.cash,
            max_leverage=metrics_tracker.account.leverage,
            universe=self._universe.name,
            look_back=self._look_back,
            data_frequency=self._data_frequency,
            start=conversions.to_proto_timestamp(self._start_dt),
            end=conversions.to_proto_timestamp(self._end_dt),
            checkpoint=conversions.to_proto_timestamp(dt),
            metrics_tracker_state=metrics_tracker.get_state(dt)
        ).SerializeToString()

    def restore_state(self, state, strategy):
        raise NotImplementedError('restore_state')

        # self.initialize(
        #     state.session_id,
        #     conversions.to_datetime(state.start_dt),
        #     conversions.to_datetime(state.end_dt),
        #     state.universe,
        #     strategy,
        #     state.capital,
        #     state.max_leverage,
        #     state.data_frequency,
        #     state.mode,
        #     state.look_back,
        #     '')

        # ss.set_state(state.session_state, self._sync_state_tracker)
        # self._current_dt = conversions.to_datetime(state.checkpoint)

    def stop(self, dt):
        # todo: disable buying stocks, only allow selling...
        #  this will gradually close positions...
        pass

    @abstractmethod
    def _update_blotter(self, blotter, broker_data):
        raise NotImplementedError

    def update_account(self, main_account):
        self._update_account(self._blotter, main_account)

    def _update_calendar(self, clock_):
        algo = self._algo

        self._calendar = calendar = clock_.calendar

        # todo: we need a better way for reloading everything
        #  we should only reload the calendar, since the data gets
        #  updated in the background...

        # reload a new calendar
        # start_dt = dt - pd.Timedelta(days=self._look_back)
        # self._end_dt = dt
        #
        # # todo: reload equity minute bar reader etc (reload bundle)
        #
        # self._calendar = calendar = self._universe.load_calendar(
        #     start_dt,
        #     dt)

        self._end_dt = end_dt = calendar.last_session

        self._last_session_close = calendar.session_close(end_dt)

        # fixme: this might not be correct
        self._start_dt = fs = calendar.first_session

        look_back = calendar.last_session - fs

        sessions = self._sessions_array
        # reload calendar if it is not the full look-back period
        # if look_back < self._look_back:
        #     self._end_dt = end = fs + pd.Timedelta(days=150)
        #     self._calendar = calendar = self._universe.load_calendar(fs, end)
        #     dt = end

        dt = clock_.session_end

        sessions.popleft()
        sessions.append(dt)

        # updates the sessions (live)
        params = self._params
        self._params = trading.SimulationParameters(
            fs,
            dt,
            calendar,
            params.capital_base,
            params.emission_rate,
            params.data_frequency,
            params.arena
        )

        # reload bundle so that it updates the calendar instance
        bundle = self._universe

        self._data_portal = data_portal = dp.DataPortal(
            asset_finder=self._asset_finder,
            trading_calendar=calendar,
            first_trading_day=fs,
            equity_minute_reader=bundle.equity_minute_bar_reader,
            equity_daily_reader=bundle.equity_daily_bar_reader,
            adjustment_reader=bundle.adjustment_reader
        )

        self._current_data = self._create_bar_data(
            data_portal,
            calendar,
            self._restrictions,
            params.data_frequency)

        dom = domain.Domain(
            self._universe.country_code,
            calendar)

        algo.set_trading_calendar(calendar)

        algo.set_pipeline_domain(dom)

        # re-initialize pipeline engine
        algo.init_engine(self._universe.pipeline_loader(dom))

    @abstractmethod
    def _update_account(self, blotter, main_account):
        raise NotImplementedError

    def update_capital(self, dt, capital):
        self._capital_changes = {dt: {'type': 'target', 'value': capital}}

    @abstractmethod
    def _create_blotter(self, session_id, engine_setup, cancel_policy):
        raise NotImplementedError(self._create_blotter.__name__)

    def get_current_dt(self):
        return self._current_dt

    def _create_bar_data(self, data_portal, calendar, restrictions, data_frequency):
        return BarData(
            data_portal=data_portal,
            simulation_dt_func=self.get_current_dt,
            data_frequency=data_frequency,
            trading_calendar=calendar,
            restrictions=restrictions)

    def _cleanup_expired_assets(self,
                                dt,
                                data_portal,
                                blotter,
                                metrics_tracker,
                                position_assets):
        """

        Parameters
        ----------
        dt
        data_portal
        blotter
        metrics_tracker: pluto.finance.metrics.tracker.MetricsTracker
        position_assets

        Returns
        -------

        """
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
            if acd is not None:
                # mirrors zipline-reloaded 2642caa7: asset dates are naive
                acd = acd.tz_localize(dt.tzinfo)
            return acd is not None and acd <= dt

        # Remove positions in any sids that have reached their auto_close date.
        assets_to_clear = [
            asset for asset in position_assets
            if past_auto_close_date(asset)]

        for asset in assets_to_clear:
            metrics_tracker.process_close_position(asset, dt, data_portal)

        # Remove open orders for any sids that have reached their auto close
        # date. These orders get processed immediately because otherwise they
        # would not be processed until the first bar of the next day.

        assets_to_cancel = [
            asset for asset in blotter._open_orders
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

    def sync_last_sale_prices(self):
        self._sync_last_sale_prices(self._metrics_tracker, self._current_dt)

    def _sync_last_sale_prices(self, metrics_tracker, dt):
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
                self._data_portal,
            )

            self._last_sync_time = dt

    def _calculate_capital_changes(self,
                                   dt,
                                   metrics_tracker,
                                   emission_rate,
                                   is_interday,
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
                    (metrics_tracker.portfolio.portfolio_value -
                     portfolio_value_adjustment))

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

        return {
            'capital_change':
                {'date': dt,
                 'type': 'cash',
                 'target': target,
                 'delta': capital_change_amount}
        }
