"""The controller side participants of a run: they subscribe to the phase events (``pluto.control.phases``) and do what the
controller called them for before the bus; each says ``done`` when its work of the phase is done.

``BrokerParticipant`` is the broker side: the book (``BrokerService``) and the strategy registry (``StrategyManager``).
The metrics are not a phase: a consumer of the stream of the book computes them (``pluto.finance.metrics.consumer``);
the book phase tells the stream where a reader may read (``BrokerService.observe``).
"""
import logbook

from pluto.bus import Bus, Delivery
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

        # what each runtime says it sent in the trade phase, by process id: (orders, batches)
        self._sent = {}

        bus.subscribe(phases.TRADE.done_topic, self._on_done)
        bus.subscribe(phases.BOOK.topic, self._on_book)
        bus.subscribe(phases.ACCOUNT.topic, self._on_account)
        bus.subscribe(phases.SUBMIT.topic, self._on_submit)

    def _on_done(self, delivery: Delivery) -> None:
        done = delivery.envelope.payload

        if not done.error:
            self._sent[done.participant] = (done.orders, done.batches)

    def _on_book(self, delivery: Delivery) -> None:
        envelope = delivery.envelope
        phases.report(
            self._bus,
            phases.BOOK,
            envelope.logical_ts,
            phases.BROKER_ID,
            lambda: self._book(envelope.payload),
            log)

    def _book(self, event) -> None:
        """The writes of the book for one signal, in the order the controller made them. The book is brought up to
        date (``revalue``) where the metrics are read (by the consumer of the stream, from its own copy), and the stream
        is told that it may read (``observe``): these are the last calls of the phase."""
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
            broker.observe(calendar_name, evt, cts)

        elif evt == SESSION_START:
            broker.start_session(calendar_name, cts)
            broker.revalue(calendar_name)
            broker.observe(calendar_name, evt, cts)

        elif evt == BEFORE_TRADING_START:
            broker.sync_last_sale_prices(calendar_name, prices, cts)

        elif evt == SESSION_END:
            broker.sync_last_sale_prices(calendar_name, prices, cts)
            broker.end_session(calendar_name, cts)

            # todo: we need to ingest data befor calling this...
            broker.revalue(calendar_name)
            broker.observe(calendar_name, evt, cts)

    def _on_account(self, delivery: Delivery) -> None:
        envelope = delivery.envelope
        phases.report(
            self._bus,
            phases.ACCOUNT,
            envelope.logical_ts,
            phases.BROKER_ID,
            lambda: self._account(envelope.payload, envelope.logical_ts),
            log)

    def _account(self, event, logical_ts) -> None:
        """The broker state of each strategy, in registration order: its fills (collected from the book's stream since
        the last tick) and the last sale prices of the tick, sent on the strategy's own topic."""
        manager = self._manager

        # the stale-batch filter of the manager (``receive_orders``): what the runtimes send
        # for another clock event than the one they are about to run is ignored
        manager.set_global_timestamp(conversions.to_datetime(event.clock.timestamp))

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

    def _check_orders(self) -> None:
        """Each runtime said how many orders, in how many batches, it sent in the tick: the manager must have received
        as many (a batch lost on the way, delivered twice, or for another clock event that was ignored, would
        otherwise go unnoticed, an empty one too)."""
        manager = self._manager
        sent = self._sent
        problems = []

        for process_id in manager.session_ids:
            expected = sent.get(process_id, (0, 0))
            received = (manager.received_orders(process_id), manager.received_batches(process_id))

            if expected != received:
                problems.append('{} sent {} orders in {} batches, the broker received {} in {}'.format(
                    process_id, *expected, *received))

        sent.clear()

        if problems:
            raise phases.PhaseError('; '.join(problems))

    def _on_submit(self, delivery: Delivery) -> None:
        envelope = delivery.envelope
        phases.report(self._bus, phases.SUBMIT, envelope.logical_ts, phases.BROKER_ID, self._submit, log)

    def _submit(self) -> None:
        """The orders the strategies sent in this tick reach the book, by strategy in registration order; then the
        manager starts the next tick (new orders and broker states)."""
        manager = self._manager
        self._check_orders()
        orders = manager.new_orders

        # todo: catch any account control errors here...
        #  given the new orders and the current portfolio and
        #  the current open orders and the last sale price, determine
        #  if we go above the maximum leverage

        # check if strategies don't go above allocated capital and leverage

        # todo: we should call the manager script to handle new orders here, before placing new orders.
        self._broker.submit(orders)

        manager.clear()
