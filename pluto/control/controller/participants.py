"""The controller side participants of a run: they subscribe to the phase events (``pluto.control.phases``) and do what the
controller called them for before the bus; each says ``done`` when its work of the phase is done.

``BrokerParticipant`` is the broker side: the book (``BrokerService``) and the strategy registry (``StrategyManager``).
The metrics are not a phase: a consumer of the stream of the book computes them (``pluto.finance.metrics.consumer``);
the book phase tells the stream where a reader may read (``BrokerService.observe``).
"""
import logbook
import pandas as pd

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

        # the submit event that is waiting for the batches of orders it names (networked bus), if any
        self._waiting = None
        # the ids of the phase events handled in this tick: a copy is ignored (see ``phases.Handled``)
        self._handled = phases.Handled()
        # the batches of orders the bus gave up on that nobody reported yet, and how many dead letters were looked at
        self._dead = []
        self._letters_seen = 0

        bus.subscribe(phases.BOOK.topic, self._on_book)
        bus.subscribe(phases.ACCOUNT.topic, self._on_account)
        bus.subscribe(phases.SUBMIT.topic, self._on_submit)
        # rank 1: after the manager's own subscription took the batch
        bus.subscribe(phases.ORDERS_TOPIC, self._on_orders_arrived, rank=1)

    def _on_book(self, delivery: Delivery) -> None:
        envelope = delivery.envelope

        if not self._handled.first(phases.step_of(envelope), envelope.msg_id):
            return  # a copy of the book event: the book is written once (revalue compounds returns)

        phases.report(
            self._bus,
            phases.BOOK,
            envelope.logical_ts,
            phases.BROKER_ID,
            lambda: self._book(envelope.payload),
            log,
            answers=envelope)

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

        if not self._handled.first(phases.step_of(envelope), envelope.msg_id):
            return  # a copy of the account event: the states are sent once

        phases.report(
            self._bus,
            phases.ACCOUNT,
            envelope.logical_ts,
            phases.BROKER_ID,
            lambda: self._account(envelope.payload, envelope.logical_ts, envelope),
            log,
            answers=envelope)

    def _account(self, event, logical_ts, envelope) -> None:
        """The broker state of each strategy, in registration order: its fills (collected from the book's stream since
        the last tick) and the last sale prices of the tick, sent on the strategy's own topic."""
        manager = self._manager

        # a batch that arrived for a tick that was closed is an error (counted by the manager, not applied), not a
        # silent drop; the stale-batch filter of the manager (``receive_orders``) takes only the batches of this tick
        late = self._batch_problems()

        if late:
            raise phases.PhaseError('; '.join(late))

        manager.set_global_timestamp(conversions.to_datetime(event.clock.timestamp))

        for process_id in manager.session_ids:
            state = manager.broker_states[process_id]
            state.last_sale_prices.extend(event.last_sale_prices)

            phases.publish_with_retry(
                self._bus,
                phases.account_topic(process_id),
                state,
                msg_id=phases.account_msg_id(process_id, envelope.msg_id),
                correlation_id=envelope.correlation_id,
                type=phases.message_type(state),
                logical_ts=logical_ts,
                producer_id=phases.BROKER_ID,
                phase=phases.ACCOUNT.rank,
                key=process_id)

    def _count_problems(self, announced) -> list[str]:
        """Each runtime announced how many orders, in how many batches, it sent in the tick (in the submit event, the
        controller saw the dones): the manager must have received as many (a batch lost on the way, delivered twice, or
        for another clock event that was ignored, would otherwise go unnoticed, an empty one too)."""
        manager = self._manager
        counts = {runtime.participant: (runtime.orders, runtime.batches) for runtime in announced}
        problems = []

        for process_id in manager.session_ids:
            expected = counts.get(process_id, (0, 0))
            received = (manager.received_orders(process_id), manager.received_batches(process_id))

            if expected != received:
                problems.append('{} sent {} orders in {} batches, the broker received {} in {}'.format(
                    process_id, *expected, *received))

        return problems

    def _short(self, announced) -> bool:
        """Whether a runtime has sent more than the manager has received so far (what is more than announced is an
        error, not something to wait for)."""
        manager = self._manager

        return any(
            manager.received_orders(runtime.participant) < runtime.orders
            or manager.received_batches(runtime.participant) < runtime.batches
            for runtime in announced)

    def _batch_problems(self) -> list[str]:
        """The batches that arrived for a tick that was already closed (counted by the manager, it does not apply them),
        and the copies of a batch that contradict it."""
        manager = self._manager

        return ['{} sent a batch of {} orders for {}, which arrived after its tick closed'.format(
            process_id, orders, pd.Timestamp(ts, tz='UTC')) for process_id, ts, orders in manager.take_late_batches()
        ] + ['{} sent two batches of orders under the id {} that differ'.format(process_id, msg_id)
             for process_id, msg_id in manager.take_conflicting_batches()]

    def _dead_batches(self) -> list[str]:
        """The batches of orders the bus gave up on (dead-lettered) since the last call, as the problems to report."""
        letters = self._bus.dead_letters()
        new = [letter for letter in letters[self._letters_seen:] if letter.envelope.topic == phases.ORDERS_TOPIC]
        self._letters_seen = len(letters)

        return ['the bus gave up on a batch of orders of {} after {} attempts: {}: {}'.format(
            letter.envelope.key, letter.attempts, letter.error_type, letter.error_message) for letter in new]

    def _check_orders(self, announced, dead) -> None:
        problems = dead + self._count_problems(announced) + self._batch_problems()

        if problems:
            raise phases.PhaseError('; '.join(problems))

    def _on_submit(self, delivery: Delivery) -> None:
        envelope = delivery.envelope

        if not self._handled.first(phases.step_of(envelope), envelope.msg_id):
            return  # a copy of the submit event: the orders reach the book once

        self._waiting = delivery.envelope
        self._submit_when_ready()

    def _on_orders_arrived(self, delivery: Delivery) -> None:
        # after the manager took the batch (rank): a submit that was waiting for batches may go on
        if self._waiting is not None:
            self._submit_when_ready()

    def _submit_when_ready(self) -> None:
        """The submit event names the orders and batches each runtime sent.

        On one stream the controller published the event only after it saw every announced batch in the log, and a log is
        a prefix, so this participant's mirror has the batches before it has the event and nothing waits. The wait is
        for a bus that delivers in other legal orders (the batch of another lane after the event: the fault-injecting
        bus, a delayed delivery): on a networked bus the phase goes on when ``strategy.orders`` brings the missing
        batches; the controller's deadline names the broker if they never come. A batch the bus gave up on (dead
        letter) is reported at once instead of waited for. In memory nothing else can arrive: what is missing is an
        error now."""
        envelope = self._waiting
        announced = envelope.payload.runtimes
        self._dead.extend(self._dead_batches())

        if self._bus.networked and self._short(announced) and not self._dead:
            return

        self._waiting = None
        dead, self._dead = self._dead, []
        phases.report(
            self._bus, phases.SUBMIT, envelope.logical_ts, phases.BROKER_ID, lambda: self._submit(announced, dead), log,
            answers=envelope)

    def _submit(self, announced, dead) -> None:
        """The orders the strategies sent in this tick reach the book, by strategy in registration order; then the
        manager starts the next tick (new orders and broker states)."""
        manager = self._manager
        self._check_orders(announced, dead)

        orders = manager.new_orders

        # todo: catch any account control errors here...
        #  given the new orders and the current portfolio and
        #  the current open orders and the last sale price, determine
        #  if we go above the maximum leverage

        # check if strategies don't go above allocated capital and leverage

        # todo: we should call the manager script to handle new orders here, before placing new orders.
        self._broker.submit(orders)

        manager.clear()
