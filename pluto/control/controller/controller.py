import logbook

from pluto.finance.markets import core

from pluto.control.controller import process_manager
from pluto.control.controller import behavior as bv

from pluto.data.data_sources import data_source as ds
from pluto.coms.utils import conversions

from protos.clock_pb2 import (
	BAR,
	TRADE_END,
	SESSION_END,
	SESSION_START,
	MINUTE_END,
	BEFORE_TRADING_START)

from protos import broker_pb2

log = logbook.Logger("Controller")


class Controller(object):
	def __init__(
			self,
			strategy_manager: process_manager.StrategyManager,
			market: core.AbstractMarket,
			metrics_tracker,
			behavior: bv.AbstractControllerBehavior):

		self._strategy_manager = strategy_manager
		self._metrics_tracker = metrics_tracker
		self._market = market
		self._behavior = behavior

	def clock_update(self, ts, evt, signals):
		strategy_manager = self._strategy_manager
		market = self._market
		behavior = self._behavior

		strategies = strategy_manager.strategies
		broker_states = strategy_manager.broker_states
		# set timestamp ahead of time so that we know which events to wait for...
		strategy_manager.set_global_timestamp(ts)
		metrics_tracker = self._metrics_tracker
		# data update hook
		behavior.ingest_watchlist_data(ts, strategy_manager, signals)
		
		last_sale_prices = behavior.get_last_sale_prices(ts, strategy_manager, signals)

		# todo: we need to optimize the control flow...
		#  we don't want data downloads and ingestion from session end
		#  events to block the execution of the other strategies
		#  we might need thread-safety (for the blotter, ledger etc.)

		# we could have a channel per calendar? 

		# todo: we need to trigger any of these functions once the data tasks have been executed!
		#  this means that we need a class to handle each process callback
		#  once all the processes have ran, we update the blotter, ledger, metrics tracker...

		# todo: instead of using a locking mechanism for the strategy manager, we should have multiple
		#  channels, one per strategy, where they can write there states.

		# todo: session_end events for data must be executed last since they are low priority.
		#  bar, trade end are high priority, since they involve trading...

		# populate broker state to send to controllableWe need to update positions before calling handle_data
		for cts, c_evt, calendar in signals:
			calendar_name = calendar.name
			market.set_calendar_timestamp(calendar_name, cts)

			# bar events
			if c_evt == BAR or c_evt == TRADE_END:
				strategy_manager.sync_last_sale_prices(calendar_name, last_sale_prices[calendar_name], cts)
				# fetch executed transactions, commissions etc.
				# todo: need to apply tax models...
				blotter = strategy_manager.get_blotter(calendar_name)

				transactions, closed_orders = market.get_transactions(
					calendar_name, blotter)

				# transactions, closed_orders = strategy_manager.get_transactions(calendar_name)

				# update states
				for transaction in transactions:
					order = transaction['order']

					strategy_manager.process_transaction(
						calendar_name,
						transaction['transaction'])

					if transaction['commission'] > 0:
						strategy_manager.process_commission(
							calendar_name,
							{
								'asset': order.asset,
								'order': order.to_dict(),
								'cost': transaction['commission']
							})

					broker_states[order.session_id].transactions.append(
						broker_pb2.TransactionCommission(
							transaction=conversions.to_proto_transaction(transaction['transaction'].to_dict()),
							commission=transaction['commission']
						))

				blotter.prune_orders(closed_orders)
				# note: here we force synchronization of sale prices
				# strategy_manager.sync_last_sale_prices(calendar_name, last_sale_prices[calendar_name], cts)

			elif c_evt == MINUTE_END:
				strategy_manager.sync_last_sale_prices(
					calendar_name,
					last_sale_prices[calendar_name],
					cts)

				metrics_tracker.end_of_bar(
					cts,
					calendar_name,
					strategy_manager.get_ledger(calendar_name))

			elif c_evt == SESSION_START:
				# todo: each session start check if leverage have changed
				ledger = strategy_manager.get_ledger(calendar_name)
				data_portal = strategy_manager.get_data_portal(calendar_name)
				strategy_manager.process_dividends(
					cts,
					calendar_name,
					data_portal.adjustment_reader)
				positions = set([position.asset
								 for position in strategy_manager.get_positions(calendar_name)
								 if position.amount != 0])
				open_orders = strategy_manager.get_open_orders(calendar_name)
				strategy_manager.handle_market_open(ts, calendar_name)
				assets_we_care_about = positions | open_orders.keys()
				if assets_we_care_about:
					splits = data_portal.get_splits(assets_we_care_about, cts)
					if splits:
						strategy_manager.process_splits(
							calendar_name,
							splits)
				# ledger.start_of_session(0)
				metrics_tracker.start_of_session(cts, calendar_name, ledger)
			elif c_evt == BEFORE_TRADING_START:
				strategy_manager.sync_last_sale_prices(
					calendar_name,
					last_sale_prices[calendar_name],
					cts)

			elif c_evt == SESSION_END:
				strategy_manager.sync_last_sale_prices(
					calendar_name,
					last_sale_prices[calendar_name],
					cts)
				strategy_manager.cleanup_expired_assets(calendar_name, cts)

				# todo: we need to ingest data befor calling this...
				metrics_tracker.end_of_session(
					cts,
					calendar_name,
					strategy_manager.get_ledger(calendar_name))

		# flatten last sale prices to send to
		values = []

		for pairs in last_sale_prices.values():
			values.extend([
				broker_pb2.AssetPrice(sid=sid, price=price)
				for sid, price in pairs])

		# update all strategies state
		for strategy in strategies:
			brs = broker_states[strategy.process_id]
			brs.last_sale_prices.extend(values)
			strategy.account_update(brs)

		# todo: we need to call all trade events first, then session end events next...
		# we could separate them into different threads: we have a thread for executing live
		# events and a thread for executing session end events...
		# session end events trigger a write to the core database, for all assets in the database
		# problem: what if there is a session start for a strategy before all its session end data
		# is ingested?

		# advance all processes
		for strategy in strategies:
			strategy.clock_update(ts, evt)

		# fixme: a process might crash, and we would be waiting forever in the strategy manager
		#  a solution would be to fix a maximum waiting time (timeout)
		#  this shouldn't be a problem in local network, if a strategy crashed due to some
		#  bug or error, everything should crash.
		# retrieve new orders
		new_orders = strategy_manager.get_orders()
		# process watchlist from strategies
		# ingest for all assets before updating watchlist...
		behavior.ingest_all_data(ts, strategy_manager, signals)
		behavior.update_watchlist(ts, strategy_manager) 

		# todo: catch any account control errors here...
		#  given the new orders and the current portfolio and
		#  the current open orders and the last sale price, determine
		#  if we go above the maximum leverage

		# check if strategies don't go above allocated capital and leverage

		# todo: call the controller script
		# self._controller_script.update()

		# todo: we should call the manager script to handle new orders here, before placing new orders.
		market.place_orders(new_orders, strategy_manager)

		# clear all events
		strategy_manager.clear()
