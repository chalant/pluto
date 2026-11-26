"""A consumer of the event stream of a book that computes metrics from it.

``StreamMetrics`` is a subscriber of the broker service (``broker.subscribe(consumer)``): it folds every event into
its own copy of the book (``stream.Replica``) and, at each observation point (``events.Observed``: the book is
complete for a SESSION_START, MINUTE_END or SESSION_END signal), calls the tracker with the ledger of that copy.

The copy is written by the fold only and read at observation points only: the portfolio is revalued lazily and each
revaluation compounds returns and pnl, so where the copy revalues (``events.Revalued``, replayed by the fold) and
where it is read is part of the result; a reader that reads at other points changes the last digits.

A failure of the tracker (or of the fold) must not reach the book's write that published the event: it is recorded in
``error`` (with ``error_sequence``, the sequence number of the event being handled) and the consumer ignores the
events that follow, since its copy of the book is no longer known to be right. What a run does with it is the run's
policy: a simulated run stops (the controller calls ``raise_if_failed`` after the book phase of each tick).
"""
from typing import Protocol

import pandas as pd

from pluto.broker_service import events, stream

from protos.clock_pb2 import (
    MINUTE_END,
    SESSION_END,
    SESSION_START)


class MetricsError(RuntimeError):
    """The metrics consumer of a book failed (see ``StreamMetrics.error``)."""


class Tracker(Protocol):
    """What the consumer needs of the tracker: it reads the ledger of a calendar at three clock events."""

    def end_of_bar(self, dt: pd.Timestamp, calendar_name: str, ledger) -> None: ...

    def start_of_session(self, dt: pd.Timestamp, calendar_name: str, ledger) -> None: ...

    def end_of_session(self, dt: pd.Timestamp, calendar_name: str, ledger) -> None: ...


class StreamMetrics(object):
    def __init__(self, start_dt: pd.Timestamp, capital: float, tracker: Tracker):
        """
        Parameters
        ----------
        start_dt: pandas.Timestamp
            Start of the book (as given to the broker service).
        capital: float
            Starting cash of the book.
        tracker
            Called at the observation points.
        """
        self._replica = stream.Replica(start_dt, capital)
        self._tracker = tracker

        self.error = None
        self.error_sequence = None

    @property
    def replica(self) -> stream.Replica:
        """The consumer's copy of the book, for tests: the consumer writes it (the fold) and reads it at observation
        points only; anything else that reads it may revalue it where the book did not, and change the results."""
        return self._replica

    def raise_if_failed(self) -> None:
        """Raises ``MetricsError`` (from the error that was recorded) if the consumer failed."""
        error = self.error

        if error is not None:
            raise MetricsError(
                'the metrics consumer failed at event {} of the stream of the book: {}: {}'.format(
                    self.error_sequence, type(error).__name__, error)) from error

    def __call__(self, stream_event: events.StreamEvent) -> None:
        if self.error is not None:
            return

        if stream_event.sequence <= self._replica.sequence:
            # delivered again (at-least-once delivery): already folded, and its observation point already read
            return

        try:
            stream.fold((stream_event,), self._replica)

            event = stream_event.event

            if isinstance(event, events.Observed):
                self._observe(stream_event.calendar_name, event)
        except Exception as e:
            self.error = e
            self.error_sequence = stream_event.sequence

    def _observe(self, calendar_name: str, observed: events.Observed) -> None:
        tracker = self._tracker
        clock_event = observed.clock_event

        if clock_event == MINUTE_END:
            read = tracker.end_of_bar
        elif clock_event == SESSION_START:
            read = tracker.start_of_session
        elif clock_event == SESSION_END:
            read = tracker.end_of_session
        else:
            raise ValueError('no read at the clock event {}'.format(clock_event))

        read(observed.dt, calendar_name, self._replica.ledgers[calendar_name])
