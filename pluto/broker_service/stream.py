"""Consumers of the stream of a book: an in-memory sink, and the fold that
rebuilds the ledgers of the book from its events."""
import copy

from zipline.finance.order import Order

from pluto.broker_service import books, events


class InMemoryStream(object):
    """A sink that keeps the events: ``broker.subscribe(stream)``."""

    def __init__(self):
        self._events = []

    def __call__(self, stream_event):
        if stream_event.sequence != len(self._events) + 1:
            raise ValueError('event {} after {} events: the stream has a gap or a repeat'.format(
                stream_event.sequence, len(self._events)))

        self._events.append(stream_event)

    def __iter__(self):
        return iter(self._events)

    def __len__(self):
        return len(self._events)


class Replica(books.Books):
    """The books of a strategy, rebuilt from its stream by ``fold``."""

    def __init__(self, start_dt, capital):
        super().__init__(start_dt, capital)

        # sequence number of the last event folded (0: none), for the book
        self.sequence = 0

        # the engine's ledgers hold the very order objects of the blotter,
        # which go on changing: an order is one object, updated in place by
        # each event that tells about it (id -> order)
        self._orders = {}

    def live_order(self, snapshot):
        order = self._orders.get(snapshot.id)

        if order is None:
            order = self._orders[snapshot.id] = copy.copy(snapshot)
        else:
            for slot in Order.__slots__:
                setattr(order, slot, getattr(snapshot, slot))

        return order


def fold(stream_events, replica):
    """Applies the events, in order, to the ledgers of ``replica`` (created as
    their calendars appear). The events must follow the ones already folded
    without a gap (ValueError); an event already folded (its sequence is not
    above ``replica.sequence``) is skipped, so folding a stream again, or its
    beginning again, leaves the book as it is.

    It rebuilds a book from the beginning of its stream. It does not continue
    from a ledger restored by ``Ledger.restore_state``: the saved state lacks
    the unpaid dividends, the payout prices of futures, the returns of the
    previous session and the transactions of the session, and restoring
    replaces the portfolio and account that the ledgers of the calendars
    share.

    Parameters
    ----------
    stream_events: iterable of events.StreamEvent
    replica: Replica
    """
    ledgers = replica.ledgers

    for stream_event in stream_events:
        sequence = stream_event.sequence

        if sequence <= replica.sequence:
            continue

        if sequence != replica.sequence + 1:
            raise ValueError('event {} after event {}: the stream has a gap'.format(
                sequence, replica.sequence))

        calendar_name = stream_event.calendar_name
        ledger_ = ledgers[calendar_name] if calendar_name in ledgers else replica.add_ledger(calendar_name)

        _apply(replica, ledger_, stream_event.event)

        replica.sequence = sequence
        for ledger_ in ledgers.values():
            ledger_.stream_sequence = sequence


def _apply(replica, ledger_, event):
    # each event is the write the broker service made, with the same ledger call
    match event:
        case events.OrderAccepted(order) | events.OrderCancelled(order) | events.OrderExpired(order):
            ledger_.process_order(replica.live_order(order))
        case events.OrderRejected():
            pass
        case events.OrderTriggered(order):
            replica.live_order(order)
        case events.Fill(order, transaction):
            ledger_.process_transaction(transaction)
            ledger_.process_order(replica.live_order(order))
        case events.Commission(asset, cost):
            ledger_.process_commission({'asset': asset, 'cost': cost})
        case events.PricesMarked(dt, prices):
            ledger_.mark_prices(prices, dt)
        case events.Dividends(dt, cash_dividends, stock_dividends):
            ledger_.process_dividends(dt, cash_dividends, stock_dividends)
        case events.SessionStarted(dt):
            ledger_.start_of_session(dt)
        case events.Split(splits):
            ledger_.process_splits(splits)
        case events.Expiry(transaction):
            ledger_.process_transaction(transaction)
        case events.Revalued():
            ledger_.revalue()
        case events.Observed():
            # a point where a reader may read: the book does not change
            pass
        case _:
            raise TypeError('not an event of a book: {!r}'.format(event))
