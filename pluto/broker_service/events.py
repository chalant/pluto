"""The events of the stream of a book, and the envelope that orders them.

Every change of the book of a strategy is published, in the order it happened,
as a ``StreamEvent``: the strategy's id, a sequence number (1, 2, 3, ... per
stream, without gaps), the calendar whose ledger changed, and the event. The
events are what ``stream.fold`` needs to rebuild the ledger of the book: each
one is one write to it. They are immutable, and the orders they carry are
snapshots, not the blotter's live objects.
"""
import dataclasses

import pandas as pd
from zipline.assets import Asset
from zipline.finance.order import Order
from zipline.finance.transaction import Transaction


@dataclasses.dataclass(frozen=True, slots=True)
class OrderAccepted:
    """A new order was accepted (open)."""
    order: Order


@dataclasses.dataclass(frozen=True, slots=True)
class OrderCancelled:
    """The strategy cancelled the order (cancel, cancel policy)."""
    order: Order


@dataclasses.dataclass(frozen=True, slots=True)
class OrderExpired:
    """The order was cancelled because its asset reached its auto close date."""
    order: Order


@dataclasses.dataclass(frozen=True, slots=True)
class OrderRejected:
    """The order was refused: nothing was recorded."""
    order: Order
    reason: str


@dataclasses.dataclass(frozen=True, slots=True)
class OrderTriggered:
    """The price reached the stop or the limit of the order, which did not fill
    (an order that fills is described by its Fill): its time and triggers
    changed. The ledger that holds the order shows the change."""
    order: Order


@dataclasses.dataclass(frozen=True, slots=True)
class Fill:
    """A transaction of the order; ``order`` is the order after the fill. The
    commission it cost is applied by the Commission event that follows the
    fills of the bar."""
    order: Order
    transaction: Transaction
    commission: float


@dataclasses.dataclass(frozen=True, slots=True)
class Commission:
    asset: Asset
    cost: float


@dataclasses.dataclass(frozen=True, slots=True)
class PricesMarked:
    """The last sale prices of the positions at ``dt``."""
    dt: pd.Timestamp
    prices: tuple[tuple[int, float], ...]


@dataclasses.dataclass(frozen=True, slots=True)
class Dividends:
    """The dividends of the held assets whose ex-date is the session ``dt``
    (possibly none): earned now, and the ones due on ``dt`` are paid."""
    dt: pd.Timestamp
    cash_dividends: tuple
    stock_dividends: tuple


@dataclasses.dataclass(frozen=True, slots=True)
class SessionStarted:
    dt: pd.Timestamp


@dataclasses.dataclass(frozen=True, slots=True)
class Split:
    splits: tuple[tuple[Asset, float], ...]


@dataclasses.dataclass(frozen=True, slots=True)
class Expiry:
    """A position closed at the auto close date of its asset."""
    transaction: Transaction


@dataclasses.dataclass(frozen=True, slots=True)
class Revalued:
    """The portfolio was brought up to date. The revaluation is lazy (on read)
    and compounds returns and pnl each time, so where it happened is part of the
    state of the book."""


@dataclasses.dataclass(frozen=True, slots=True)
class Observed:
    """An observation point: the book of the calendar is complete for the clock
    event of the signal (the last write of its phase, the portfolio revalued
    where a reader is about to read it), so a reader of the book may read now.
    ``clock_event`` is the clock event (``protos.clock_pb2.EventType``: SESSION_START,
    MINUTE_END or SESSION_END) and ``dt`` the time of the calendar's signal.
    It changes nothing in the book: the fold only advances past it."""
    clock_event: int
    dt: pd.Timestamp


type Event = (OrderAccepted | OrderCancelled | OrderExpired | OrderRejected | OrderTriggered | Fill
              | Commission | PricesMarked | Dividends | SessionStarted | Split | Expiry | Revalued | Observed)


@dataclasses.dataclass(frozen=True, slots=True)
class StreamEvent:
    strategy_id: str | None
    sequence: int
    calendar_name: str
    event: Event
