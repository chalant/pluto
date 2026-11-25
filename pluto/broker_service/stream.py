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
    their calendars appear). An event the ledger of its calendar has already
    folded (its sequence is not above the ledger's ``stream_sequence``) is
    skipped: folding a stream again, or from the start after restoring a saved
    state, leaves the book as it is.

    Parameters
    ----------
    stream_events: iterable of events.StreamEvent
    replica: Replica
    """
    ledgers = replica.ledgers

    for stream_event in stream_events:
        calendar_name = stream_event.calendar_name
        ledger_ = ledgers[calendar_name] if calendar_name in ledgers else replica.add_ledger(calendar_name)

        if stream_event.sequence <= ledger_.stream_sequence:
            continue

        _apply(replica, ledger_, stream_event.event)
        ledger_.stream_sequence = stream_event.sequence


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
        case _:
            raise TypeError('not an event of a book: {!r}'.format(event))
