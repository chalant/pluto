"""The simulated broker service of the controller side.

It owns the book of the strategy (the portfolio, the positions, the account and,
per calendar, a ledger and the controller's copy of the open orders, the
blotter) and is its single writer: nothing else changes a ledger or a blotter
of the controller side. The controller tells it what happens; it does not decide
when:

- ``submit`` (orders and cancellations relayed by the strategies),
- ``set_time`` and ``sync_last_sale_prices`` (market data of the event),
- ``match`` (fill the open orders against the bar of the calendar),
- ``start_session`` (dividends and splits) and ``end_session`` (assets that
  expired).

Every write to the book is published to the subscribers, in order, as an
``events.StreamEvent`` with the sequence number of the stream of the strategy
(see ``events``; ``stream.fold`` rebuilds the ledgers from them). The sequence
number of the last event of a ledger is kept in the ledger
(``Ledger.stream_sequence``, part of its saved state). The ledgers and blotters
can be read (``get_ledger``, ``get_positions``, ``get_open_orders``), not written.

The order of the writes inside a call is the order of the simulation of
Zipline, and the floating point sums depend on it: all the transactions of a
bar are applied, then all the commissions.
"""
import copy

from zipline.finance.order import ORDER_STATUS

from pluto.broker_service import books, events
from pluto.trading_calendars import calendar_utils as cu


def _snapshot(order):
    # the blotter and the ledger keep mutating their order: events keep a copy
    return copy.copy(order)


def _trigger_state(order):
    # what the market changes of an order that it does not fill
    return order.dt, order.stop, order.stop_reached, order.limit_reached


class BrokerService(object):
    def __init__(self, asset_finder, start_dt, capital, data_frequency):
        """
        Parameters
        ----------
        asset_finder: pluto.assets.assets.AssetFinder
        start_dt: pandas.Timestamp
            Start of the book.
        capital: float
            Starting cash of the book.
        data_frequency: str
        """
        self._asset_finder = asset_finder
        self._data_frequency = data_frequency

        # one book: the calendars' ledgers share the portfolio, the
        # positions and the account
        self._books = books.Books(start_dt, capital)
        self._ledgers = self._books.ledgers

        self._market = None
        self._blotters = {}
        self._data_portals = {}

        self._strategy_id = None
        self._sequence = 0
        self._subscribers = []

    def attach_market(self, market):
        """The market that fills the open orders.

        Parameters
        ----------
        market: pluto.finance.markets.core.AbstractMarket
        """
        self._market = market

    def set_strategy(self, strategy_id):
        """The strategy that owns the book (one per book)."""
        if self._strategy_id not in (None, strategy_id):
            raise ValueError(
                'the book belongs to strategy {}, cannot add {}'.format(
                    self._strategy_id, strategy_id))
        self._strategy_id = strategy_id

    def subscribe(self, callback):
        """Calls ``callback(stream_event)`` for every event published from now on."""
        self._subscribers.append(callback)

    def add_data_portal(self, calendar_name, data_portal):
        self._data_portals[calendar_name] = data_portal

    def add_blotter(self, calendar_name, blotter):
        self._blotters[calendar_name] = blotter

    def add_ledger(self, calendar_name):
        ledger_ = self._books.add_ledger(calendar_name)
        ledger_.on_revalue = lambda: self._publish(calendar_name, events.Revalued())

    def get_ledger(self, calendar_name):
        return self._ledgers[calendar_name]

    def get_positions(self, calendar_name):
        for position in self._ledgers[calendar_name].position_tracker.get_positions().values():
            yield position

    def get_open_orders(self, calendar_name):
        return self._blotters[calendar_name].open_orders

    def set_time(self, calendar_name, dt):
        """The time of the calendar: the market's, and the time stamped on the
        orders that the blotter cancels (expired assets, cancel policies), as
        zipline's algo.on_dt_changed."""
        self._market.set_calendar_timestamp(calendar_name, dt)
        self._blotters[calendar_name].set_date(dt)

    def sync_last_sale_prices(self, calendar_name, prices, dt):
        """
        Parameters
        ----------
        prices: iterable of (sid, price)
        """
        prices = tuple(prices)

        self._ledgers[calendar_name].mark_prices(prices, dt)
        self._publish(calendar_name, events.PricesMarked(dt, prices))

    def submit(self, orders):
        """Orders relayed by the strategies, in the order they were placed. A
        cancelled order is a cancellation of the order of that id.

        Parameters
        ----------
        orders: iterable of zipline.finance.order.Order
        """
        for order in orders:
            # fixme: find a better way of getting calendar names (from disk ?)
            calendar_name = cu.resolve_alias(order.asset.exchange)
            blotter = self._blotters[calendar_name]
            ledger_ = self._ledgers[calendar_name]

            if order.status == ORDER_STATUS.CANCELLED:
                self._cancel(calendar_name, blotter, ledger_, order)
                continue

            if order.id in blotter.orders:
                # at-least-once delivery: an order of a known id is not a new order
                rejected = _snapshot(order)
                rejected.reject('duplicate order id')
                self._publish(
                    calendar_name,
                    events.OrderRejected(rejected, rejected.reason))
                continue

            blotter.open_orders[order.asset].append(order)
            blotter.orders[order.id] = order
            # record the order in the period it was placed (as zipline's
            # AlgorithmSimulator does with metrics_tracker.process_order)
            ledger_.process_order(order)
            self._publish(calendar_name, events.OrderAccepted(_snapshot(order)))

    def _cancel(self, calendar_name, blotter, ledger_, order):
        # a cancellation relayed by the strategy's blotter (cancel_order,
        # cancel policy): cancel the controller's copy, as zipline's
        # blotter.cancel; an order placed and cancelled in the same bar is
        # only recorded.
        if order.id in blotter.orders:
            # the time of the cancellation is the one the strategy's blotter
            # stamped (the cancel policy runs at the end of the session, the
            # order is relayed with the next bar)
            cancelled_at = order.dt
            blotter.cancel(order.id, relay_status=False)
            order = blotter.orders[order.id]
            order.dt = cancelled_at
        else:
            blotter.orders[order.id] = order
        ledger_.process_order(order)
        self._publish(calendar_name, events.OrderCancelled(_snapshot(order)))

    def match(self, calendar_name):
        """Fills the open orders of the calendar against its current bar, and
        writes the fills to the book."""
        blotter = self._blotters[calendar_name]
        ledger_ = self._ledgers[calendar_name]

        open_orders = [order for orders in blotter.open_orders.values() for order in orders]
        triggers = [_trigger_state(order) for order in open_orders]

        transactions, closed_orders = self._market.get_transactions(
            calendar_name, blotter)

        # the market changes the orders whose stop or limit the price reaches;
        # the ledger holds these very orders and shows it
        filled = {transaction['transaction'].order_id for transaction in transactions}
        for order, trigger in zip(open_orders, triggers):
            if order.id not in filled and _trigger_state(order) != trigger:
                self._publish(calendar_name, events.OrderTriggered(_snapshot(order)))

        # all the transactions first, then all the commissions, in this order
        # (as zipline's AlgorithmSimulator every_bar); interleaving them
        # changes the floating-point summation of the cash.
        commissions = []
        for transaction in transactions:
            order = transaction['order']
            txn = transaction['transaction']
            commission = transaction['commission']

            ledger_.process_transaction(txn)
            ledger_.process_order(blotter.orders[txn.order_id])

            if commission > 0:
                commissions.append({
                    'asset': order.asset,
                    'cost': commission
                })

            self._publish(
                calendar_name,
                events.Fill(_snapshot(order), txn, commission))

        for commission in commissions:
            ledger_.process_commission(commission)
            self._publish(
                calendar_name,
                events.Commission(commission['asset'], commission['cost']))

        blotter.prune_orders(closed_orders)

    def start_session(self, calendar_name, dt):
        """Dividends of the session, the new session of the ledger, splits."""
        ledger_ = self._ledgers[calendar_name]
        blotter = self._blotters[calendar_name]
        data_portal = self._data_portals[calendar_name]

        # todo: each session start check if leverage have changed
        held_sids = set(ledger_.position_tracker.positions)
        cash_dividends = []
        stock_dividends = []
        if held_sids:
            adjustment_reader = data_portal.adjustment_reader
            cash_dividends = adjustment_reader.get_dividends_with_ex_date(
                held_sids,
                dt,
                self._asset_finder)
            stock_dividends = adjustment_reader.get_stock_dividends_with_ex_date(
                held_sids,
                dt,
                self._asset_finder)
        ledger_.process_dividends(dt, cash_dividends, stock_dividends)
        self._publish(
            calendar_name,
            events.Dividends(dt, tuple(cash_dividends), tuple(stock_dividends)))

        positions = set([position.asset
                         for position in self.get_positions(calendar_name)
                         if position.amount != 0])
        open_orders = blotter.open_orders
        ledger_.start_of_session(dt)
        self._publish(calendar_name, events.SessionStarted(dt))
        assets_we_care_about = positions | open_orders.keys()
        if assets_we_care_about:
            splits = data_portal.get_splits(assets_we_care_about, dt)
            if splits:
                blotter.process_splits(splits)
                ledger_.process_splits(splits)
                self._publish(calendar_name, events.Split(tuple(splits)))

    def end_session(self, calendar_name, dt):
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

        blotter = self._blotters[calendar_name]
        data_portal = self._data_portals[calendar_name]
        ledger_ = self._ledgers[calendar_name]

        # todo: check if this block of code works with real-time data...

        position_assets = self._asset_finder.retrieve_all([position['sid'] for position in ledger_.positions])

        # Remove positions in any sids that have reached their auto_close date.
        assets_to_clear = [asset for asset in position_assets if past_auto_close_date(asset)]

        for asset in assets_to_clear:
            txn = ledger_.close_position(asset, dt, data_portal, self._data_frequency)
            if txn is not None:
                self._publish(calendar_name, events.Expiry(txn))

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
        for order in blotter.new_orders.copy():
            if order.status == ORDER_STATUS.CANCELLED:
                ledger_.process_order(order)
                blotter.new_orders.remove(order)
                self._publish(calendar_name, events.OrderExpired(_snapshot(order)))

    def _publish(self, calendar_name, event):
        self._sequence += 1
        self._ledgers[calendar_name].stream_sequence = self._sequence

        stream_event = events.StreamEvent(
            self._strategy_id,
            self._sequence,
            calendar_name,
            event)

        for callback in self._subscribers:
            callback(stream_event)
