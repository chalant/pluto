"""The events a book publishes, and the envelope that orders them.

Every change of the book of a strategy is published, in the order it happened,
as a ``StreamEvent``: the strategy's id, a sequence number (1, 2, 3, ... per
stream, without gaps), the calendar whose ledger changed, and the event.
Events are immutable; the orders they carry are snapshots, not the blotter's
live objects.
"""
import dataclasses

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
class Fill:
    """A transaction of the order, with the commission it cost (applied by its
    own Commission event); ``order`` is the order after the fill."""
    order: Order
    transaction: Transaction
    commission: float


@dataclasses.dataclass(frozen=True, slots=True)
class StreamEvent:
    strategy_id: str | None
    sequence: int
    calendar_name: str
    event: (OrderAccepted | OrderCancelled | OrderExpired | OrderRejected | Fill)
