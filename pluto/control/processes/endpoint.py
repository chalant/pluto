"""The strategy runtime on the bus of a run.

``RuntimeEndpoint`` is what a runtime does with the phases (``pluto.control.phases``): in the ``account`` phase it applies
the broker state (fills and last sale prices) it receives on its own topic, in the ``trade`` phase it runs the clock event
(the strategy's callbacks), which sends the watchlist and the orders back through ``send`` onto ``strategy.watchlist`` and
``strategy.orders`` (``pluto.control.processes.in_memory.BrokerStub``). It says done in the trade phase when the clock event
has run and its batch of orders is published: the done carries the number of orders it sent, which the broker side checks
against what it received. A runtime that sent no batch says so in its done (the controllable always sends one, empty
when nothing was ordered or when it has ended).

The runtime itself (``StrategyStub``, the controllable behind it) is not aware of the bus. In a deployment where the runtime
is another process it is this object, built there, that subscribes.
"""
import logbook

from pluto.bus import Bus
from pluto.control import phases

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
        self._orders = 0

        bus.subscribe(phases.account_topic(self._id), self._on_account)
        bus.subscribe(phases.TRADE.topic, self._on_trade)

    def send(self, topic: str, message) -> None:
        """What the runtime sends to the controller side in the trade phase: its watchlist (``strategy.watchlist``)
        and its batch of orders (``strategy.orders``), for the clock event ``message.ts``."""
        if topic == phases.ORDERS_TOPIC:
            self._batches += 1
            self._orders += len(message.orders)

        self._bus.publish(
            topic,
            message,
            type=phases.message_type(message),
            logical_ts=message.ts.ToNanoseconds(),
            producer_id=self._id,
            phase=phases.TRADE.rank,
            key=self._id)

    def _on_account(self, delivery) -> None:
        envelope = delivery.envelope
        phases.report(
            self._bus,
            phases.ACCOUNT,
            envelope.logical_ts,
            self._id,
            lambda: self._account(envelope.payload),
            log)

    def _account(self, state) -> phases.Summary:
        self._process.account_update(state)

        return phases.Summary(
            tuple(commission.transaction.order_id for commission in state.transactions),
            len(state.last_sale_prices))

    def _on_trade(self, delivery) -> None:
        envelope = delivery.envelope
        self._batches = 0
        self._orders = 0

        try:
            self._process.clock_event(envelope.payload)
        except Exception as e:
            phases.fail(self._bus, phases.TRADE, envelope.logical_ts, self._id, e, log)
            return

        if self._batches == 0:
            phases.publish_done(
                self._bus,
                phases.TRADE,
                envelope.logical_ts,
                self._id,
                error='sent no batch of orders (a runtime must run its clock event synchronously)')
        else:
            phases.publish_done(
                self._bus, phases.TRADE, envelope.logical_ts, self._id, phases.Summary(count=self._orders))
