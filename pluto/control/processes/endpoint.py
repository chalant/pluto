"""The strategy runtime on the bus of a run.

``RuntimeEndpoint`` is what a runtime does with the phases (``pluto.control.phases``): in the ``account`` phase it applies
the broker state (fills and last sale prices) it receives on its own topic, in the ``trade`` phase it runs the clock event
(the strategy's callbacks), which sends the watchlist and the orders back on ``strategy.watchlist`` and
``strategy.orders`` (``pluto.control.processes.in_memory.BrokerStub``). It says done in the trade phase when its batch
of orders is on the bus: the done lists the ids of the orders in it.

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

        bus.subscribe(phases.account_topic(self._id), self._on_account)
        bus.subscribe(phases.TRADE.topic, self._on_trade)
        bus.subscribe(phases.ORDERS_TOPIC, self._on_orders)

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

        try:
            self._process.clock_event(envelope.payload)
        except Exception as e:
            phases.fail(self._bus, phases.TRADE, envelope.logical_ts, self._id, e, log)

    def _on_orders(self, delivery) -> None:
        envelope = delivery.envelope

        # the batches of the other runtimes are theirs to report
        if envelope.key == self._id:
            batch = envelope.payload

            phases.publish_done(
                self._bus,
                phases.TRADE,
                envelope.logical_ts,
                self._id,
                phases.Summary(tuple(order.id for order in batch.orders), len(batch.orders)))
