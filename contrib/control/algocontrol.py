from contextlib2 import ExitStack
from copy import copy
from logbook import Logger, Processor

from zipline.finance.order import ORDER_STATUS
from zipline.protocol import BarData
from zipline.utils.api_support import ZiplineAPI
from zipline.algorithm import TradingAlgorithm

from six import viewkeys

from contrib.control.clock import (
	BAR,
	SESSION_START,
	SESSION_END,
	BEFORE_TRADING_START_BAR,
	MINUTE_END,
	LIQUIDATE,
	STOP
)

log = Logger('Algorithm Control')


class AlgoController(object):
	''' a clock observer that controls the TradingAlgorithm object'''

	EMISSION_TO_PERF_KEY_MAP = {
		'minute': 'minute_perf',
		'daily': 'daily_perf'
	}

	def __init__(self, algo, perf_handler, universe_func, benchmark_source):
		if not isinstance(algo, TradingAlgorithm):
			raise TypeError("Expected {} got {}".format(TradingAlgorithm, type(algo)))

		self._sims_params = algo.sim_params

		self._algo = algo
		self._blotter = algo.blotter
		self._metrics_tracker = algo.get_metrics_tracker()
		self._emission_rate = self._get_emission_rate(self._metrics_tracker)
		self._data_frequency = algo.data_frequency
		self._current_data = self._create_bar_data(universe_func)
		self._data_portal = algo.data_portal
		self._simulator_dt = None
		self._perf_handler = perf_handler
		self._exit_stack = None
		self._current_data = self._create_bar_data(universe_func)

		self._benchmark_source = benchmark_source

		# =============
		# Logging Setup
		# =============

		# Processor function for injecting the algo_dt into
		# user prints/logs.
		def inject_algo_dt(record):
			if 'algo_dt' not in record.extra:
				record.extra['algo_dt'] = self._simulation_dt

		self._processor = Processor(inject_algo_dt)

	def _get_emission_rate(self, metrics_tracker):
		return metrics_tracker.emission_rate

	def _get_handle_data(self, event_manager):
		return event_manager.handle_data

	def get_simulation_dt(self):
		return self._simulation_dt

	def _create_bar_data(self, universe_func, data_portal, get_simulation_dt, data_frequency, calendar,restrictions):
		return BarData(
			data_portal=data_portal,
			simulation_dt_func=get_simulation_dt,
			data_frequency=data_frequency,
			trading_calendar=calendar,
			restrictions=restrictions,
			universe_func=universe_func)

	def _every_bar(self, algo, metrics_tracker, dt_to_use, current_data,
				   handle_data, calculate_minute_capital_changes):
		for capital_change in calculate_minute_capital_changes(dt_to_use):
			yield capital_change

		self._simulation_dt = dt_to_use
		# called every tick (minute or day).
		algo.on_dt_changed(dt_to_use)

		blotter = self._blotter

		# handle any transactions and commissions coming out new orders
		# placed in the last bar
		new_transactions, new_commissions, closed_orders = blotter.get_transactions(current_data)

		blotter.prune_orders(closed_orders)

		for transaction in new_transactions:
			#todo: these calls aren't necessary... since these are handled by the server.
			metrics_tracker.process_transaction(transaction)

			# since this order was modified, record it
			order = blotter.orders[transaction.order_id]
			metrics_tracker.process_order(order)

		for commission in new_commissions:
			metrics_tracker.process_commission(commission)

		handle_data(algo, current_data, dt_to_use)

		# grab any new orders from the blotter, then clear the list.
		# this includes cancelled orders.
		new_orders = blotter.new_orders
		blotter.new_orders = []

		# if we have any new orders, record them so that we know
		# in what perf period they were placed.
		for new_order in new_orders:
			metrics_tracker.process_order(new_order)

	def _once_a_day(self, algo, metrics_tracker, emission_rate, midnight_dt, current_data,
					data_portal):
		# process any capital changes that came overnight
		for capital_change in algo.calculate_capital_changes(
				midnight_dt, emission_rate=emission_rate,
				is_interday=True):
			yield capital_change

		# set all the timestamps
		self._simulator.simulation_dt = midnight_dt
		algo.on_dt_changed(midnight_dt)

		metrics_tracker.handle_market_open(
			midnight_dt,
			algo.data_portal,
		)

		# handle any splits that impact any positions or any open orders.
		assets_we_care_about = (
				viewkeys(metrics_tracker.positions) |
				viewkeys(algo.blotter.open_orders)
		)

		if assets_we_care_about:
			splits = data_portal.get_splits(assets_we_care_about,
											midnight_dt)
			if splits:
				algo.blotter.process_splits(splits)
				metrics_tracker.handle_splits(splits)

	def _choose_execute_order_cancellation_policy(self, data_frequency, blotter):
		if data_frequency == 'minute':
			def execute_order_cancellation_policy():
				blotter.execute_cancel_policy(SESSION_END)

			func = execute_order_cancellation_policy
		else:
			def execute_order_cancellation_policy():
				pass

			func = execute_order_cancellation_policy
		return func

	def _choose_calculate_minute_capital_changes(self, data_frequency, algo, emission_rate):
		if data_frequency == 'minute':
			def calculate_minute_capital_changes(dt):
				return algo.calculate_capital_changes(dt, emission_rate=emission_rate, is_interday=False)

			func = calculate_minute_capital_changes
		else:
			def calculate_minute_capital_changes(dt):
				return []

			func = calculate_minute_capital_changes
		return func

	def on_clock_event(self, dt, event):
		algo = self._algo
		metrics_tracker = self._metrics_tracker
		handle_data = self._get_handle_data(algo.event_manager)
		emission_rate = self._emission_rate
		data_portal = self._data_portal
		blotter = self._blotter
		data_frequency = self._data_frequency
		cmc = self._choose_calculate_minute_capital_changes(self._data_frequency, algo, emission_rate)
		execute_order_cancellation_policy = self._choose_execute_order_cancellation_policy(data_frequency, blotter)

		if self._exit_stack is None:
			self._exit_stack = ExitStack()
			stack = self._exit_stack
			stack.callback(self.on_exit)
			stack.enter_context(self._processor)
			stack.enter_context(ZiplineAPI(self._algo))

		if event == BAR:
			self._update_perf_handler(self._every_bar(algo, metrics_tracker, dt, self._current_data, handle_data,
													  cmc))
		elif event == SESSION_START:
			self._update_perf_handler(self._once_a_day(algo, metrics_tracker, emission_rate, dt, self._current_data,
													   data_portal))
		elif event == SESSION_END:
			# End of the session.
			positions = metrics_tracker.positions
			position_assets = algo.asset_finder.retrieve_all(positions)
			self._cleanup_expired_assets(dt, position_assets)

			execute_order_cancellation_policy()
			algo.validate_account_controls()

			self._update_perf_handler_one(self._get_daily_message(dt, algo, metrics_tracker))
		elif event == BEFORE_TRADING_START_BAR:
			self._simulation_dt = dt
			algo.on_dt_changed(dt)
			algo.before_trading_start(self._current_data)
		elif event == MINUTE_END:
			minute_msg = self._get_minute_message(
				dt,
				algo,
				metrics_tracker,
			)
			self._update_perf_handler_one(minute_msg)
		elif event == FINISH:
			risk_message = metrics_tracker.handle_simulation_end(
				self.data_portal,
			)

			self._update_perf_handler_one(risk_message)
			self._exit_stack.close()
			self._exit_stack = None

	def _update_perf_handler(self, perf_generator):
		for perf in perf_generator:
			self._update_perf_handler_one(perf)

	def _update_perf_handler_one(self, perf):
		self._perf_handler.handle_perf(perf)

	def _get_daily_message(self, dt, algo, metrics_tracker):
		"""
		Get a perf message for the given datetime.
		"""
		perf_message = metrics_tracker.handle_market_close(
			dt,
			self.data_portal,
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

	def _cleanup_expired_assets(self, dt, position_assets):
		"""
		Clear out any assets that have expired before starting a new sim day.

		Performs two functions:

		1. Finds all assets for which we have open orders and clears any
		   orders whose assets are on or after their auto_close_date.

		2. Finds all assets for which we have positions and generates
		   close_position events for any assets that have reached their
		   auto_close_date.
		"""
		algo = self.algo

		def past_auto_close_date(asset):
			acd = asset.auto_close_date
			return acd is not None and acd <= dt

		# Remove positions in any sids that have reached their auto_close date.
		assets_to_clear = [asset for asset in position_assets if past_auto_close_date(asset)]
		metrics_tracker = algo.metrics_tracker
		data_portal = self.data_portal
		for asset in assets_to_clear:
			metrics_tracker.process_close_position(asset, dt, data_portal)

		# Remove open orders for any sids that have reached their auto close
		# date. These orders get processed immediately because otherwise they
		# would not be processed until the first bar of the next day.
		blotter = self._blotter
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
