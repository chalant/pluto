"""The broker service publishes the stream of the book of the strategy: every
write to the ledger of the controller side, in order, with a sequence number.
Folding that stream (pluto.broker_service.stream.fold) must rebuild the ledger
state the engine ends with, exactly: the serialised states are compared byte
for byte, for every example and every scenario run through the in-memory
controller path."""
import dataclasses
import hashlib
import unittest
from unittest import mock

import pandas as pd
from zipline import protocol
from zipline.assets import Equity, ExchangeInfo
from zipline.finance.order import Order, ORDER_STATUS
from zipline.finance.transaction import Transaction

from pluto import examples
from pluto.broker_service import events, service, stream
from pluto.control.processes import processes
from pluto.finance import ledger, position_tracker
from pluto.finance.blotter import simulation_blotter

from protos import ledger_state_pb2

from tests.pluto_scenarios import SCENARIO_MODULES
from tests.test_pluto_examples import _run
from tests.test_pluto_zipline_oracle import ZiplinePlutoFixture

START = pd.Timestamp('2014-01-02', tz='UTC')
DT = pd.Timestamp('2014-01-03 21:00', tz='UTC')
CAPITAL = 100000.0

# RunSetup('test'): the session id of the runs of tests.test_pluto_examples._run
SESSION_ID = 'test'

ALL_MODULES = dict(examples.EXAMPLE_MODULES)
ALL_MODULES.update(SCENARIO_MODULES)
assert len(ALL_MODULES) == len(examples.EXAMPLE_MODULES) + len(SCENARIO_MODULES)


class _RecordingBrokerService(service.BrokerService):
    """The broker service of a run, with its stream kept, and checked against a
    fold of the stream at the end of every session (what the engine's ledger
    holds then: orders, transactions, saved state)."""

    created = []

    def __init__(self, *args):
        super().__init__(*args)
        self.stream = stream.InMemoryStream()
        self.subscribe(self.stream)

        self.replica = stream.Replica(args[1], args[2])
        self._unfolded = []
        self.subscribe(self._unfolded.append)
        # (orders, transactions) counted at each comparison; what differed
        self.compared = []
        self.mismatches = []

        self.created.append(self)

    def end_session(self, calendar_name, dt):
        super().end_session(calendar_name, dt)
        # the controller revalues here, before a reader of the book (the metrics consumer) reads
        self.revalue(calendar_name)
        self._compare(calendar_name, dt)

    def _compare(self, calendar_name, dt):
        stream.fold(self._unfolded, self.replica)
        self._unfolded.clear()

        engine = self.get_ledger(calendar_name)
        folded = self.replica.ledgers[calendar_name]

        self.compared.append((len(engine.orders()), len(engine.transactions())))
        for what, engine_value, folded_value in (
                ('orders', engine.orders(), folded.orders()),
                ('transactions', engine.transactions(), folded.transactions()),
                ('orders(dt)', engine.orders(dt), folded.orders(dt)),
                ('state', engine.get_state(dt), folded.get_state(dt))):
            if engine_value != folded_value:
                self.mismatches.append((dt, what))


class StreamOfRunsTests(ZiplinePlutoFixture):
    _runs = {}

    def _run(self, name):
        """(broker service, run arguments) of the run of an example or a
        scenario; run once per class."""
        runs = type(self)._runs
        if name not in runs:
            _RecordingBrokerService.created.clear()
            with mock.patch.object(service, 'BrokerService', _RecordingBrokerService):
                _run(self._pluto_tempdir, name, self._environ, modules=ALL_MODULES)
            [broker] = _RecordingBrokerService.created
            runs[name] = (broker, ALL_MODULES[name][0])
        return runs[name]

    def _kinds(self, name):
        broker, _ = self._run(name)
        return {type(stream_event.event) for stream_event in broker.stream}

    def check_fold(self, name):
        broker, args = self._run(name)

        # what the folded book was found to differ in, at the end of each session
        self.assertEqual(broker.mismatches, [])
        if any(isinstance(e.event, events.Fill) for e in broker.stream):
            # the comparisons were not empty ones: there were orders to compare
            self.assertTrue(any(orders for orders, _ in broker.compared))
            self.assertTrue(any(transactions for _, transactions in broker.compared))

        ledger_ = broker.get_ledger('XNYS')
        # the checkpoint: revalue (it is published), then read
        broker.revalue('XNYS')
        engine_state = ledger_.get_state(DT)

        folded = stream.Replica(args['start'], args['capital_base'])
        stream.fold(broker.stream, folded)
        folded_ledger = folded.ledgers['XNYS']
        folded_state = folded_ledger.get_state(DT)

        self.assertEqual(folded_state, engine_state)

        # and what the ledger reports of the session: orders, transactions
        self.assertEqual(folded_ledger.orders(), ledger_.orders())
        self.assertEqual(folded_ledger.transactions(), ledger_.transactions())
        for stream_event in broker.stream:
            if isinstance(stream_event.event, events.Fill):
                dt = stream_event.event.transaction.dt
                self.assertEqual(folded_ledger.orders(dt), ledger_.orders(dt))

        # the saved state carries the sequence of the last event
        saved = ledger_state_pb2.LedgerState.FromString(engine_state)
        self.assertEqual(saved.version, ledger.STATE_VERSION)
        self.assertGreater(len(broker.stream), 0)
        self.assertEqual(saved.stream_sequence, len(broker.stream))

        # folding the stream again changes nothing
        stream.fold(broker.stream, folded)
        self.assertEqual(folded.ledgers['XNYS'].get_state(DT), engine_state)

        # the book of one strategy, nothing was refused
        process_id = processes.make_process_id(name, SESSION_ID)
        for stream_event in broker.stream:
            self.assertEqual(stream_event.strategy_id, process_id)
            self.assertNotIsInstance(stream_event.event, events.OrderRejected)

        # the orders of the strategy are numbered 1, 2, 3... after its namespace
        namespace = hashlib.md5(process_id.encode()).hexdigest()[:16]
        numbers = set()
        for stream_event in broker.stream:
            if isinstance(stream_event.event, (
                    events.OrderAccepted, events.OrderCancelled, events.OrderExpired)):
                order_id = stream_event.event.order.id
                self.assertEqual(order_id[:len(namespace)], namespace)
                numbers.add(int(order_id[len(namespace):], 16))
        self.assertEqual(sorted(numbers), list(range(1, len(numbers) + 1)))

    def test_orders_are_cancelled(self):
        self.assertIn(events.OrderCancelled, self._kinds('cancel_orders'))

    def test_delisted_assets_expire(self):
        kinds = self._kinds('delisting')
        self.assertIn(events.Expiry, kinds)
        self.assertIn(events.OrderExpired, kinds)

    def test_splits(self):
        self.assertIn(events.Split, self._kinds('splits'))

    def test_dividends(self):
        broker, _ = self._run('dividends')
        earned = [stream_event.event for stream_event in broker.stream
                  if isinstance(stream_event.event, events.Dividends)
                  and (stream_event.event.cash_dividends or stream_event.event.stock_dividends)]
        self.assertTrue(earned)

    def test_fills_and_commissions(self):
        kinds = self._kinds('commission_per_trade')
        self.assertIn(events.Fill, kinds)
        self.assertIn(events.Commission, kinds)


def _make_fold_test(name):
    def test(self):
        self.check_fold(name)
    test.__name__ = 'test_fold_' + name
    return test


for _name in sorted(ALL_MODULES):
    setattr(StreamOfRunsTests, 'test_fold_' + _name, _make_fold_test(_name))
del _name


def _equity(sid=1, exchange='XNYS'):
    return Equity(
        sid,
        ExchangeInfo(exchange, exchange, 'US'),
        symbol='TEST{}'.format(sid),
        asset_name='TEST',
        start_date=pd.Timestamp('2004-01-02', tz='UTC'),
        end_date=pd.Timestamp('2014-12-31', tz='UTC'),
        first_traded=pd.Timestamp('2004-01-02', tz='UTC'),
        auto_close_date=pd.Timestamp('2015-01-01', tz='UTC'),
        tick_size=0.01,
        multiplier=1.0)


def _without_sequence(state):
    message = ledger_state_pb2.LedgerState.FromString(state)
    message.ClearField('stream_sequence')
    return message.SerializeToString()


class BrokerServiceTests(unittest.TestCase):
    """Order handling of the service by itself."""

    def setUp(self):
        self.asset = _equity()
        self.broker = service.BrokerService(None, START, CAPITAL, 'daily')
        self.stream = stream.InMemoryStream()
        self.broker.subscribe(self.stream)
        self.broker.add_ledger('XNYS')
        self.broker.add_blotter('XNYS', simulation_blotter.SimulationBlotter(None))

    def _order(self, order_id='o1', amount=10):
        return Order('session', DT, self.asset, amount, id_=order_id)

    def _kinds(self):
        return [type(stream_event.event) for stream_event in self.stream]

    def test_an_order_is_accepted_once(self):
        self.broker.submit([self._order()])
        self.broker.submit([self._order(amount=99)])

        self.assertEqual(
            self._kinds(), [events.OrderAccepted, events.OrderRejected])
        [_, rejected] = list(self.stream)
        self.assertEqual(rejected.event.order.status, ORDER_STATUS.REJECTED)
        self.assertEqual(rejected.event.reason, 'duplicate order id')

        # the first order is the one recorded
        [recorded] = self.broker.get_ledger('XNYS').orders()
        self.assertEqual(recorded['amount'], 10)
        self.assertEqual(len(self.broker.get_open_orders('XNYS')[self.asset]), 1)

    def test_a_cancellation_cancels_the_order(self):
        self.broker.submit([self._order()])
        cancelled = self._order()
        cancelled.cancel()
        self.broker.submit([cancelled])

        self.assertEqual(
            self._kinds(), [events.OrderAccepted, events.OrderCancelled])
        [recorded] = self.broker.get_ledger('XNYS').orders()
        self.assertEqual(recorded['status'], ORDER_STATUS.CANCELLED)
        self.assertEqual(self.broker.get_open_orders('XNYS'), {})

    def test_events_are_snapshots(self):
        self.broker.submit([self._order()])
        self.broker._blotters['XNYS'].orders['o1'].filled = 5

        [accepted] = list(self.stream)
        self.assertEqual(accepted.event.order.filled, 0)

    def test_sequence_and_strategy(self):
        self.broker.set_strategy('s1')
        self.broker.submit([self._order('o1'), self._order('o2')])

        self.assertEqual(
            [(e.strategy_id, e.sequence, e.calendar_name) for e in self.stream],
            [('s1', 1, 'XNYS'), ('s1', 2, 'XNYS')])
        self.assertEqual(self.broker.get_ledger('XNYS').stream_sequence, 2)

        with self.assertRaises(ValueError):
            self.broker.set_strategy('s2')

    def test_the_stream_refuses_a_gap_and_a_repeat(self):
        self.broker.submit([self._order()])
        [first] = list(self.stream)
        sink = stream.InMemoryStream()

        with self.assertRaises(ValueError):
            sink(dataclasses.replace(first, sequence=2))

        sink(first)
        with self.assertRaises(ValueError):
            sink(first)

    def test_a_cancellation_is_idempotent(self):
        self.broker.submit([self._order()])
        cancelled = self._order()
        cancelled.cancel()
        self.broker.submit([cancelled])
        state = self.broker.get_ledger('XNYS').get_state(DT)

        later = self._order()
        later.cancel()
        later.dt = DT + pd.Timedelta(days=1)
        self.broker.submit([later])

        self.assertEqual(
            self._kinds(), [events.OrderAccepted, events.OrderCancelled])
        self.assertEqual(self.broker.get_ledger('XNYS').get_state(DT), state)

    def test_a_cancellation_of_a_filled_order_changes_nothing(self):
        market = _FillOnce()
        self.broker.attach_market(market)
        self.broker.submit([self._order()])
        self.broker.set_time('XNYS', DT)
        # the market fills the order of the book
        order = self.broker._blotters['XNYS'].orders['o1']
        order.filled = 10
        market.fills = [{
            'order': order,
            'transaction': Transaction(self.asset, 10, DT, 100.0, 'o1'),
            'commission': 0.0}]
        self.broker.match('XNYS')
        self.broker.revalue('XNYS')
        state = _without_sequence(self.broker.get_ledger('XNYS').get_state(DT))
        count = len(self.stream)

        cancelled = self._order()
        cancelled.cancel()
        cancelled.dt = DT + pd.Timedelta(days=1)
        self.broker.submit([cancelled])

        [rejected] = [e.event for e in self.stream][count:]
        self.assertIsInstance(rejected, events.OrderRejected)
        self.assertEqual(rejected.reason, 'cancel of an order that is not open')
        [recorded] = self.broker.get_ledger('XNYS').orders()
        self.assertEqual(recorded['status'], ORDER_STATUS.FILLED)
        self.assertEqual(recorded['dt'], DT)
        self.assertEqual(
            _without_sequence(self.broker.get_ledger('XNYS').get_state(DT)), state)

    def test_a_cancellation_of_an_unknown_order_is_recorded(self):
        cancelled = self._order()
        cancelled.cancel()
        self.broker.submit([cancelled])

        self.assertEqual(self._kinds(), [events.OrderCancelled])
        [recorded] = self.broker.get_ledger('XNYS').orders()
        self.assertEqual(recorded['status'], ORDER_STATUS.CANCELLED)

    def test_the_open_orders_are_copies_and_reading_them_changes_nothing(self):
        self.broker.submit([self._order()])

        open_orders = self.broker.get_open_orders('XNYS')
        open_orders[self.asset][0].filled = 5
        self.assertEqual(self.broker.get_open_orders('XNYS')[self.asset][0].filled, 0)

        other = Equity(2, ExchangeInfo('XNYS', 'XNYS', 'US'), symbol='OTHER')
        self.assertNotIn(other, self.broker.get_open_orders('XNYS'))
        self.assertNotIn(other, self.broker._blotters['XNYS'].open_orders)

    def test_the_book_keeps_its_own_copy_of_a_submitted_order(self):
        order = self._order()
        self.broker.submit([order])
        order.filled = 5

        [recorded] = self.broker.get_ledger('XNYS').orders()
        self.assertEqual(recorded['filled'], 0)
        self.assertEqual(self.broker.get_open_orders('XNYS')[self.asset][0].filled, 0)


class _FillOnce(object):
    """A market that fills what it is told to, at once."""

    def __init__(self):
        self.fills = []

    def set_calendar_timestamp(self, calendar_name, dt):
        pass

    def get_transactions(self, calendar_name, blotter):
        # the fills of the calendar (those without a calendar are anybody's)
        fills = [fill for fill in self.fills if fill.get('calendar', calendar_name) == calendar_name]
        self.fills = [fill for fill in self.fills if fill not in fills]
        return fills, [fill['order'] for fill in fills]


class RevaluationTests(unittest.TestCase):
    """The portfolio is revalued when it is read, and each revaluation compounds
    returns and pnl: where it happened changes the last digits of the state, so
    the stream carries it: the broker service revalues explicitly (``revalue``),
    publishing ``Revalued``; reading the ledger publishes nothing."""

    PRICES = (95.1, 99.91, 98.99)

    def _book(self, revalue_between=True):
        market = _FillOnce()
        broker = service.BrokerService(None, START, CAPITAL, 'daily')
        sink = stream.InMemoryStream()
        broker.subscribe(sink)
        broker.attach_market(market)
        broker.add_ledger('XNYS')
        broker.add_blotter('XNYS', simulation_blotter.SimulationBlotter(None))

        asset = _equity()
        broker.submit([Order('session', DT, asset, 10, id_='o1')])
        broker.set_time('XNYS', DT)
        # the market fills the order of the book
        order = broker._blotters['XNYS'].orders['o1']
        order.filled = 10
        market.fills = [{
            'order': order,
            'transaction': Transaction(asset, 10, DT, 100.0, 'o1'),
            'commission': 1.0}]
        broker.match('XNYS')

        first, second, third = self.PRICES
        broker.sync_last_sale_prices('XNYS', [(asset.sid, first)], DT)
        if revalue_between:
            broker.revalue('XNYS')
        broker.sync_last_sale_prices('XNYS', [(asset.sid, second)], DT)
        if revalue_between:
            broker.revalue('XNYS')
        broker.sync_last_sale_prices('XNYS', [(asset.sid, third)], DT)
        broker.revalue('XNYS')
        return broker, sink

    @staticmethod
    def _without_sequence(state):
        message = ledger_state_pb2.LedgerState.FromString(state)
        message.ClearField('stream_sequence')
        return message.SerializeToString()

    def test_the_fold_revalues_where_the_book_did(self):
        broker, sink = self._book()
        engine_state = broker.get_ledger('XNYS').get_state(DT)
        self.assertEqual(
            [type(e.event) for e in sink].count(events.Revalued), 3)

        folded = stream.Replica(START, CAPITAL)
        stream.fold(sink, folded)

        self.assertEqual(folded.ledgers['XNYS'].get_state(DT), engine_state)

    def test_where_the_revaluations_happen_changes_the_state(self):
        every, _ = self._book()
        once, _ = self._book(revalue_between=False)

        self.assertNotEqual(
            self._without_sequence(every.get_ledger('XNYS').get_state(DT)),
            self._without_sequence(once.get_ledger('XNYS').get_state(DT)))

    def test_revaluing_a_clean_ledger_publishes_nothing(self):
        broker, sink = self._book()
        count = len(sink)

        broker.revalue('XNYS')

        self.assertEqual(len(sink), count)

    def test_reading_the_ledger_after_a_revaluation_publishes_nothing(self):
        broker, sink = self._book()
        count = len(sink)
        ledger_ = broker.get_ledger('XNYS')

        ledger_.portfolio
        ledger_.account
        ledger_.returns
        ledger_.get_state(DT)

        self.assertEqual(len(sink), count)

    def test_a_subscriber_that_publishes_does_not_overtake_the_others(self):
        broker, sink = self._book(revalue_between=False)
        late = []
        asset = _equity()
        republished = []

        def republish(stream_event):
            # as a handler of the next step might: change the book while handling
            if isinstance(stream_event.event, events.PricesMarked) and not republished:
                republished.append(stream_event)
                broker.sync_last_sale_prices('XNYS', [(asset.sid, 97.0)], DT)

        broker.subscribe(republish)
        broker.subscribe(late.append)
        broker.sync_last_sale_prices('XNYS', [(asset.sid, 96.0)], DT)

        # `late` saw both events, in sequence (the second was queued, not
        # delivered ahead of the first)
        self.assertEqual([e.sequence for e in late], [len(sink) - 1, len(sink)])


class _Trigger(_FillOnce):
    """A market whose price reaches the limit of every open order, without
    filling any."""

    LATER = pd.Timestamp('2014-01-06 21:00', tz='UTC')

    def get_transactions(self, calendar_name, blotter):
        for orders in blotter.open_orders.values():
            for order in orders:
                order.limit_reached = True
                order.dt = self.LATER
        return [], []


class OrderTriggerTests(unittest.TestCase):
    """An order whose limit is reached and does not fill changes (its time),
    and the ledger that holds it shows it."""

    def _book(self, market):
        broker = service.BrokerService(None, START, CAPITAL, 'daily')
        sink = stream.InMemoryStream()
        broker.subscribe(sink)
        broker.attach_market(market)
        broker.add_ledger('XNYS')
        broker.add_blotter('XNYS', simulation_blotter.SimulationBlotter(None))

        broker.submit([Order('session', DT, _equity(), 10, limit=95.0, id_='o1')])
        broker.set_time('XNYS', DT)
        broker.match('XNYS')
        return broker, sink

    def test_a_triggered_order_is_published_and_folded(self):
        broker, sink = self._book(_Trigger())

        self.assertEqual(
            [type(e.event) for e in sink], [events.OrderAccepted, events.OrderTriggered])
        [recorded] = broker.get_ledger('XNYS').orders()
        self.assertEqual(recorded['dt'], _Trigger.LATER)

        folded = stream.Replica(START, CAPITAL)
        stream.fold(sink, folded)
        folded_ledger = folded.ledgers['XNYS']

        self.assertEqual(folded_ledger.orders(), broker.get_ledger('XNYS').orders())
        self.assertEqual(
            folded_ledger.get_state(DT), broker.get_ledger('XNYS').get_state(DT))

    def test_an_order_that_does_not_change_is_not_published(self):
        _, sink = self._book(_FillOnce())

        self.assertEqual([type(e.event) for e in sink], [events.OrderAccepted])


class TwoCalendarTests(unittest.TestCase):
    """One book, two calendars: the ledgers share the portfolio and the stream
    has one sequence for both."""

    CALENDARS = ('XNYS', 'XLON')

    def _book(self):
        market = _FillOnce()
        broker = service.BrokerService(None, START, CAPITAL, 'daily')
        sink = stream.InMemoryStream()
        broker.subscribe(sink)
        broker.attach_market(market)

        for sid, calendar_name in enumerate(self.CALENDARS, 1):
            broker.add_ledger(calendar_name)
            broker.add_blotter(calendar_name, simulation_blotter.SimulationBlotter(None))

            asset = _equity(sid, calendar_name)
            order = Order('session', DT, asset, 10 * sid, id_='o{}'.format(sid))
            broker.submit([order])
            broker.set_time(calendar_name, DT)
            order = broker._blotters[calendar_name].orders[order.id]
            order.filled = 10 * sid
            market.fills = [{
                'calendar': calendar_name,
                'order': order,
                'transaction': Transaction(asset, 10 * sid, DT, 100.0 + sid, order.id),
                'commission': 1.0}]
            broker.match(calendar_name)
            broker.sync_last_sale_prices(calendar_name, [(sid, 101.5 + sid)], DT)
            broker.revalue(calendar_name)
        return broker, sink

    def test_the_fold_rebuilds_both_ledgers(self):
        broker, sink = self._book()
        folded = stream.Replica(START, CAPITAL)
        stream.fold(sink, folded)

        for calendar_name in self.CALENDARS:
            self.assertEqual(
                folded.ledgers[calendar_name].get_state(DT),
                broker.get_ledger(calendar_name).get_state(DT))
            self.assertEqual(
                folded.ledgers[calendar_name].orders(),
                broker.get_ledger(calendar_name).orders())

    def test_every_ledger_carries_the_sequence_of_the_book(self):
        broker, sink = self._book()
        folded = stream.Replica(START, CAPITAL)
        stream.fold(sink, folded)

        ledgers = [broker.get_ledger(name) for name in self.CALENDARS]
        ledgers += [folded.ledgers[name] for name in self.CALENDARS]
        for ledger_ in ledgers:
            saved = ledger_state_pb2.LedgerState.FromString(ledger_.get_state(DT))
            self.assertEqual(saved.stream_sequence, len(sink))
        self.assertEqual(folded.sequence, len(sink))


class FoldTests(unittest.TestCase):
    def _events(self):
        broker = service.BrokerService(None, START, CAPITAL, 'daily')
        sink = stream.InMemoryStream()
        broker.subscribe(sink)
        broker.add_ledger('XNYS')
        broker.add_blotter('XNYS', simulation_blotter.SimulationBlotter(None))
        broker.sync_last_sale_prices('XNYS', [], DT)
        broker.revalue('XNYS')
        broker.sync_last_sale_prices('XNYS', [], DT)
        return list(sink)

    def test_a_gap_is_refused(self):
        first, second, third = self._events()
        folded = stream.Replica(START, CAPITAL)

        with self.assertRaisesRegex(ValueError, 'gap'):
            stream.fold([first, third], folded)

        self.assertEqual(folded.sequence, 1)

    def test_a_stream_can_be_folded_in_pieces_and_again(self):
        events_ = self._events()
        whole = stream.Replica(START, CAPITAL)
        stream.fold(events_, whole)

        pieces = stream.Replica(START, CAPITAL)
        stream.fold(events_[:2], pieces)
        stream.fold(events_, pieces)
        stream.fold(events_[1:], pieces)

        self.assertEqual(pieces.sequence, 3)
        self.assertEqual(
            pieces.ledgers['XNYS'].get_state(DT), whole.ledgers['XNYS'].get_state(DT))


class LedgerStateVersionTests(unittest.TestCase):
    def _ledger(self):
        return ledger.Ledger(
            START,
            protocol.Portfolio(START, CAPITAL),
            protocol.Account(),
            position_tracker.PositionTracker(protocol.Positions()))

    def _state(self, **fields):
        state = ledger_state_pb2.LedgerState.FromString(self._ledger().get_state(DT))
        for name, value in fields.items():
            setattr(state, name, value)
        return state.SerializeToString()

    def test_the_sequence_is_saved_and_restored(self):
        original = self._ledger()
        original.stream_sequence = 7

        restored = self._ledger()
        restored.restore_state(original.get_state(DT))

        self.assertEqual(restored.stream_sequence, 7)

    def test_a_state_without_sequence_reads_as_zero(self):
        # version 1: before the stream
        state = ledger_state_pb2.LedgerState.FromString(self._state(version=1))
        state.ClearField('stream_sequence')

        restored = self._ledger()
        restored.stream_sequence = 5
        restored.restore_state(state.SerializeToString())

        self.assertEqual(restored.stream_sequence, 0)

    def test_other_versions_are_refused(self):
        for version in (0, ledger.STATE_VERSION + 1):
            with self.assertRaisesRegex(ValueError, 'format version {}'.format(version)):
                self._ledger().restore_state(self._state(version=version))

    def test_the_error_says_why(self):
        with self.assertRaisesRegex(ValueError, 'widened to 64 bits'):
            self._ledger().restore_state(self._state(version=0))

        with self.assertRaisesRegex(ValueError, 'newer version'):
            self._ledger().restore_state(self._state(version=ledger.STATE_VERSION + 1))
