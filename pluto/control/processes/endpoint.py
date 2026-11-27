"""The strategy runtime on the bus of a run.

``RuntimeEndpoint`` is what a runtime does with the phases (``pluto.control.phases``): in the ``account`` phase it applies
the broker state (fills and last sale prices) it receives on its own topic, in the ``trade`` phase it runs the clock event
(the strategy's callbacks), which sends the orders back (``send_orders``) onto ``strategy.orders``
(``pluto.control.processes.in_memory.BrokerStub``). It says done in the
trade phase when the clock event has run and its batch of orders is published: the done carries the number of orders and
of batches it sent, which the broker side checks against what it received. A runtime that sent no batch says so in its
done (the controllable always sends one, empty when nothing was ordered or when it has ended).

The runtime itself (``StrategyStub``, the controllable behind it) is not aware of the bus. In a deployment where the runtime
is another process it is this object, built there, that subscribes.
"""
import logbook

from pluto.bus import Bus, Delivery
from pluto.control import phases

from protos import broker_pb2

log = logbook.Logger('RuntimeEndpoint')


class RuntimeEndpoint(object):
    def __init__(self, bus: Bus, process):
        """
        Parameters
        ----------
        process: pluto.control.processes.processes.StrategyStub
        """
        self._bus = bus
        self._process = process
        self._id = process.process_id

        # what the runtime sent in the trade phase it is running
        self._batches = 0
        self._event = None  # the trade event being handled: its id and step are those of its batches
        self._orders = 0

        # the tick (logical time) of the last broker state applied
        self._account_ts = None

        # the ids of the events and states handled in this tick: a copy is ignored (see ``phases.Handled``)
        self._handled = phases.Handled()

        bus.subscribe(phases.account_topic(self._id), self._on_account)
        bus.subscribe(phases.TRADE.topic, self._on_trade)

    def send_orders(self, batch: broker_pb2.OrderBatch) -> None:
        """The batch of orders of the runtime for the clock event ``batch.ts``, on ``strategy.orders``. Its id is the
        trade event being handled, the tick and the number of the batch among those of this clock event, so a publish
        that is retried is the same message and a receiver can tell a copy from the next batch."""
        self._batches += 1
        self._orders += len(batch.orders)

        event = self._event

        phases.publish_with_retry(
            self._bus,
            phases.ORDERS_TOPIC,
            batch,
            msg_id=phases.orders_msg_id(self._id, event.msg_id, self._batches),
            correlation_id=event.correlation_id,
            type=phases.message_type(batch),
            logical_ts=batch.ts.ToNanoseconds(),
            producer_id=self._id,
            phase=phases.TRADE.rank,
            key=self._id)

    def _on_account(self, delivery: Delivery) -> None:
        envelope = delivery.envelope

        if not self._handled.first(phases.step_of(envelope), envelope.msg_id):
            return  # a copy of the broker state: applying a tick's state twice would count its fills twice

        phases.report(
            self._bus,
            phases.ACCOUNT,
            envelope.logical_ts,
            self._id,
            lambda: self._account(envelope.payload, envelope.logical_ts),
            log,
            answers=envelope)

    def _account(self, state, logical_ts) -> None:
        self._process.account_update(state)
        self._account_ts = logical_ts

    def _on_trade(self, delivery: Delivery) -> None:
        envelope = delivery.envelope

        if not self._handled.first(phases.step_of(envelope), envelope.msg_id):
            return  # a copy of the trade event: the strategy runs its clock event once

        # the explicit dependency on the account phase: the strategy runs on the account of this tick or not at all
        # (the controller publishes the trade event only after the account dones, a bus that reorders must not
        # make a runtime trade on a stale account)
        if self._account_ts != envelope.logical_ts:
            error = 'the trade event of tick {} came before the account state of the tick (last applied: {})'.format(
                envelope.logical_ts, self._account_ts)
            log.error('{}: {}', self._id, error)
            phases.publish_done(
                self._bus, phases.TRADE, envelope.logical_ts, self._id, error=error, answers=envelope)
            return

        self._batches = 0
        self._orders = 0
        self._event = envelope

        try:
            self._process.clock_event(envelope.payload)
        except Exception as e:
            phases.fail(self._bus, phases.TRADE, envelope.logical_ts, self._id, e, log, answers=envelope)
            return

        if self._batches == 0:
            phases.publish_done(
                self._bus,
                phases.TRADE,
                envelope.logical_ts,
                self._id,
                error='sent no batch of orders (a runtime must run its clock event synchronously)',
                answers=envelope)
        else:
            phases.publish_done(
                self._bus,
                phases.TRADE,
                envelope.logical_ts,
                self._id,
                self._orders,
                self._batches,
                answers=envelope)
