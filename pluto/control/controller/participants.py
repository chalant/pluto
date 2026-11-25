"""The controller side participants of a run: they subscribe to the phase events (``pluto.control.phases``) and do what the
controller called them for before the bus; each says ``done`` with what it published in the phase.

``BrokerParticipant`` is the broker side: the book (``BrokerService``) and the strategy registry (``StrategyManager``).
``MetricsParticipant`` is the metrics tracker's side: it reads the ledger of the book.
"""
import logbook

from pluto.bus import Bus
from pluto.broker_service import events
from pluto.coms.utils import conversions
from pluto.control import phases
from pluto.control.controller import process_manager

from protos.clock_pb2 import (
    BAR,
    TRADE_END,
    SESSION_END,
    SESSION_START,
    MINUTE_END,
    BEFORE_TRADING_START)

log = logbook.Logger('Participants')


class BrokerParticipant(object):
    def __init__(self, bus: Bus, manager: process_manager.StrategyManager):
        self._bus = bus
        self._manager = manager
        self._broker = broker = manager.broker

        # what the book published since the phase began
        self._fills = []
        self._events = 0
        self._sequence = 0

        # the orders each runtime says it sent in the trade phase, by process id
        self._sent = {}

        broker.subscribe(self._on_stream)
        bus.subscribe(phases.DONE_TOPIC, self._on_done)
        bus.subscribe(phases.BOOK.topic, self._on_book)
        bus.subscribe(phases.ACCOUNT.topic, self._on_account)
        bus.subscribe(phases.SUBMIT.topic, self._on_submit)

    def _on_stream(self, stream_event) -> None:
        self._events += 1
        self._sequence = stream_event.sequence

        if isinstance(stream_event.event, events.Fill):
            self._fills.append(stream_event.event.order.id)

    def _on_done(self, delivery) -> None:
        done = delivery.envelope.payload

        if done.phase == phases.TRADE.topic and not done.error:
            self._sent[done.participant] = done.count

    def _begin(self) -> None:
        self._fills = []
        self._events = 0

    def _summary(self) -> phases.Summary:
        return phases.Summary(tuple(self._fills), self._events, self._sequence)

    def _on_book(self, delivery) -> None:
        envelope = delivery.envelope
        phases.report(
            self._bus,
            phases.BOOK,
            envelope.logical_ts,
            phases.BROKER_ID,
            lambda: self._book(envelope.payload),
            log)

    def _book(self, event) -> phases.Summary:
        """The writes of the book for one signal, in the order the controller made them. The book is brought up to
        date (``revalue``) where the metrics tracker will read it: it must be the last write of the phase."""
        self._begin()

        broker = self._broker
        signal = event.signal
        calendar_name = signal.calendar_name
        cts = phases.pandas_timestamp(signal.timestamp)
        evt = signal.event
        prices = [(price.sid, price.price) for price in event.last_sale_prices]

        broker.set_time(calendar_name, cts)

        # bar events
        if evt == BAR or evt == TRADE_END:
            broker.sync_last_sale_prices(calendar_name, prices, cts)
            # fetch executed transactions, commissions etc.
            # todo: need to apply tax models...
            broker.match(calendar_name)

        elif evt == MINUTE_END:
            broker.sync_last_sale_prices(calendar_name, prices, cts)
            broker.revalue(calendar_name)

        elif evt == SESSION_START:
            broker.start_session(calendar_name, cts)
            broker.revalue(calendar_name)

        elif evt == BEFORE_TRADING_START:
            broker.sync_last_sale_prices(calendar_name, prices, cts)

        elif evt == SESSION_END:
            broker.sync_last_sale_prices(calendar_name, prices, cts)
            broker.end_session(calendar_name, cts)

            # todo: we need to ingest data befor calling this...
            broker.revalue(calendar_name)

        return self._summary()

    def _on_account(self, delivery) -> None:
        envelope = delivery.envelope
        phases.report(
            self._bus,
            phases.ACCOUNT,
            envelope.logical_ts,
            phases.BROKER_ID,
            lambda: self._account(envelope.payload, envelope.logical_ts),
            log)

    def _account(self, event, logical_ts) -> phases.Summary:
        """The broker state of each strategy, in registration order: its fills (collected from the book's stream since
        the last tick) and the last sale prices of the tick, sent on the strategy's own topic."""
        manager = self._manager

        # the clock event the strategies are about to run: the orders of another one are ignored
        manager.set_global_timestamp(conversions.to_datetime(event.clock.timestamp))

        ids = []
        states = 0

        for process_id in manager.session_ids:
            state = manager.broker_states[process_id]
            state.last_sale_prices.extend(event.last_sale_prices)

            self._bus.publish(
                phases.account_topic(process_id),
                state,
                type=phases.message_type(state),
                logical_ts=logical_ts,
                producer_id=phases.BROKER_ID,
                phase=phases.ACCOUNT.rank,
                key=process_id)

            ids.extend(commission.transaction.order_id for commission in state.transactions)
            states += 1

        return phases.Summary(tuple(ids), states)

    def _check_orders(self) -> None:
        """Each runtime said how many orders it sent in the tick: the manager must have received as many (a batch lost
        on the way, or one for another clock event that was ignored, would otherwise go unnoticed)."""
        manager = self._manager
        sent = self._sent
        problems = []

        for process_id in manager.session_ids:
            expected = sent.get(process_id)
            received = manager.received_orders(process_id)

            if expected != received:
                problems.append('{} sent {} orders, the broker received {}'.format(process_id, expected, received))

        sent.clear()

        if problems:
            raise phases.PhaseError('; '.join(problems))

    def _on_submit(self, delivery) -> None:
        envelope = delivery.envelope
        phases.report(self._bus, phases.SUBMIT, envelope.logical_ts, phases.BROKER_ID, self._submit, log)

    def _submit(self) -> phases.Summary:
        """The orders the strategies sent in this tick reach the book, by strategy in registration order; then the
        manager starts the next tick (new orders, watchlist and broker states)."""
        self._begin()

        manager = self._manager
        self._check_orders()
        orders = manager.new_orders
        ids = tuple(order.id for order in orders)

        # todo: catch any account control errors here...
        #  given the new orders and the current portfolio and
        #  the current open orders and the last sale price, determine
        #  if we go above the maximum leverage

        # check if strategies don't go above allocated capital and leverage

        # todo: we should call the manager script to handle new orders here, before placing new orders.
        self._broker.submit(orders)

        manager.clear()

        return phases.Summary(ids, self._events, self._sequence)


class MetricsParticipant(object):
    def __init__(self, bus: Bus, manager: process_manager.StrategyManager, metrics_tracker):
        self._bus = bus
        self._broker = manager.broker
        self._metrics_tracker = metrics_tracker

        bus.subscribe(phases.METRICS.topic, self._on_metrics)

    def _on_metrics(self, delivery) -> None:
        envelope = delivery.envelope
        phases.report(
            self._bus,
            phases.METRICS,
            envelope.logical_ts,
            phases.METRICS_ID,
            lambda: self._observe(envelope.payload),
            log)

    def _observe(self, event) -> phases.Summary:
        """The tracker reads the ledger of the calendar (revalued by the book at the end of its phase)."""
        signal = event.signal
        calendar_name = signal.calendar_name
        cts = phases.pandas_timestamp(signal.timestamp)
        evt = signal.event
        tracker = self._metrics_tracker

        if evt == MINUTE_END:
            observe = tracker.end_of_bar
        elif evt == SESSION_START:
            observe = tracker.start_of_session
        elif evt == SESSION_END:
            observe = tracker.end_of_session
        else:
            return phases.Summary()

        observe(cts, calendar_name, self._broker.get_ledger(calendar_name))

        return phases.Summary(count=1)
