from zipline import protocol

from pluto.finance import ledger, position_tracker


class Books(object):
    """The ledgers of the book of a strategy, one per calendar. They share the
    portfolio, the positions and the account (one book, whatever the number of
    calendars); the broker service writes them as events happen, ``stream.fold``
    rebuilds them from the events."""

    def __init__(self, start_dt, capital):
        self._start_dt = start_dt

        self._portfolio = protocol.Portfolio(start_dt, capital)
        self._positions = protocol.Positions()
        self._account = protocol.Account()

        self.ledgers = {}

    def add_ledger(self, calendar_name):
        tracker = position_tracker.PositionTracker(self._positions)

        self.ledgers[calendar_name] = ledger_ = ledger.Ledger(
            self._start_dt,
            self._portfolio,
            self._account,
            tracker)

        return ledger_
