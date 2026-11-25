from pluto.finance.markets import core

from pluto.control import phases
from pluto.control.controller import process_manager
from pluto.control.controller import behavior as bv
from pluto.control.controller import participants

from protos import broker_pb2, clock_pb2, phases_pb2


def _asset_prices(pairs):
    return [broker_pb2.AssetPrice(sid=sid, price=price) for sid, price in pairs]


class Controller(object):
    """Owns the clock of the run and drives it through the phases of ``pluto.control.phases``, on the bus of the
    strategy manager: at each clock event it publishes one event per phase and waits for the ``done`` of every
    participant before the next (``PhaseError`` if one is missing). The components do the work:
    the broker service writes the book, the metrics tracker reads it, the strategies' runtimes trade."""

    def __init__(
            self,
            strategy_manager: process_manager.StrategyManager,
            market: core.AbstractMarket,
            metrics_tracker,
            behavior: bv.AbstractControllerBehavior):

        self._strategy_manager = strategy_manager
        self._behavior = behavior

        # the broker service fills the open orders against this market
        strategy_manager.broker.attach_market(market)

        # in process, the broker side and the metrics tracker are participants on the bus of the run
        bus = strategy_manager.bus
        participants.BrokerParticipant(bus, strategy_manager)
        participants.MetricsParticipant(bus, strategy_manager, metrics_tracker)
        self._driver = phases.PhaseDriver(bus)

    def clock_update(self, ts, evt, signals):
        strategy_manager = self._strategy_manager
        behavior = self._behavior
        driver = self._driver

        # data update hook
        behavior.ingest_watchlist_data(ts, strategy_manager, signals)

        # read before any write of this tick: the prices of the positions the book holds now
        last_sale_prices = behavior.get_last_sale_prices(ts, strategy_manager, signals)

        # todo: we need to optimize the control flow...
        #  we don't want data downloads and ingestion from session end
        #  events to block the execution of the other strategies
        #  we might need thread-safety (for the blotter, ledger etc.)

        # todo: we need to trigger any of these functions once the data tasks have been executed!
        #  this means that we need a class to handle each process callback
        #  once all the processes have ran, we update the blotter, ledger, metrics tracker...

        # todo: session_end events for data must be executed last since they are low priority.
        #  bar, trade end are high priority, since they involve trading...

        ts_ns = ts.value
        stamp = phases.proto_timestamp(ts)

        # the book is updated before the strategies run, so that they see the positions and fills of this event;
        # one signal after the other: the calendars' ledgers share a portfolio
        for cts, c_evt, calendar in signals:
            calendar_name = calendar.name
            signal = clock_pb2.Signal(
                timestamp=phases.proto_timestamp(cts),
                event=c_evt,
                calendar_name=calendar_name)

            driver.drive(
                phases.BOOK,
                phases_pb2.SignalEvent(
                    timestamp=stamp,
                    signal=signal,
                    last_sale_prices=_asset_prices(last_sale_prices.get(calendar_name, ()))),
                ts_ns,
                (phases.BROKER_ID,),
                key=calendar_name)

            driver.drive(
                phases.METRICS,
                phases_pb2.SignalEvent(timestamp=stamp, signal=signal),
                ts_ns,
                (phases.METRICS_ID,),
                key=calendar_name)

        # the runtimes of the strategies: a missing done of one aborts the run (fixme of the barrier before the bus:
        # a process might crash and we would wait forever)
        runtimes = tuple(strategy_manager.session_ids)
        clock = clock_pb2.ClockEvent(timestamp=stamp, event=evt)

        # flatten last sale prices to send to the strategies
        values = []

        for pairs in last_sale_prices.values():
            values.extend(_asset_prices(pairs))

        # update all strategies state
        driver.drive(
            phases.ACCOUNT,
            phases_pb2.AccountEvent(clock=clock, last_sale_prices=values),
            ts_ns,
            (phases.BROKER_ID,) + runtimes)

        # todo: we need to call all trade events first, then session end events next...
        # we could separate them into different threads: we have a thread for executing live
        # events and a thread for executing session end events...
        # session end events trigger a write to the core database, for all assets in the database
        # problem: what if there is a session start for a strategy before all its session end data
        # is ingested?

        # advance all processes; their new orders reach the manager on the bus
        driver.drive(phases.TRADE, clock, ts_ns, runtimes)

        # process watchlist from strategies
        # ingest for all assets before updating watchlist...
        behavior.ingest_all_data(ts, strategy_manager, signals)
        behavior.update_watchlist(ts, strategy_manager)

        # new orders reach the book
        driver.drive(phases.SUBMIT, clock, ts_ns, (phases.BROKER_ID,))
