"""Calendar helpers of Pluto, on top of exchange_calendars.

Pluto runs on one calendar instance per run, bounded to the run's date range
(``get_calendar_in_range``), and looks up the cached instance of the latest run
with ``get_calendar``. exchange_calendars (like zipline-reloaded) labels
sessions without a time zone; callers pass UTC-labelled or naive timestamps and
get naive sessions back.
"""
import exchange_calendars as ec
import pandas as pd
from exchange_calendars import calendar_utils as _ec_utils

_cache = {}

# Pluto's own exchange aliases (for example the test bundle's 'YAHOO'), looked up
# before exchange_calendars' aliases. They are not registered in
# exchange_calendars' global dispatcher, where zipline's examples register a
# 'YAHOO' calendar instance of their own.
_aliases = {}


def resolve_alias(name):
    """Resolve an alias (for example 'NYSE' or 'YAHOO') to a canonical calendar name."""
    seen = set()
    while name in _aliases:
        if name in seen:
            raise ValueError("Cyclic calendar alias: {!r}".format(name))
        seen.add(name)
        name = _aliases[name]
    return _ec_utils.global_calendar_dispatcher.resolve_alias(name)


def register_alias(alias, name):
    """Register ``alias`` for the calendar ``name``, replacing an earlier one."""
    _aliases[alias] = name


def deregister_alias(alias):
    _aliases.pop(alias, None)


def to_session_label(dt):
    """Timestamp -> the time-zone naive midnight label exchange_calendars uses."""
    dt = pd.Timestamp(dt)
    if dt.tz is not None:
        dt = dt.tz_convert("UTC").tz_localize(None)
    return dt.normalize()


def get_calendar_in_range(name, start_dt, end_dt=None, cache=False):
    """Get a calendar whose sessions lie between ``start_dt`` and ``end_dt``.

    Parameters
    ----------
    name : str
    start_dt : pandas.Timestamp
    end_dt : pandas.Timestamp, optional
        Defaults to ten days after ``start_dt``.
    cache : bool
        Remember the instance as the one ``get_calendar`` returns for ``name``.

    Returns
    -------
    exchange_calendars.ExchangeCalendar
        Right-sided, like zipline's calendars.
    """
    start_dt = to_session_label(start_dt)
    if end_dt is None:
        end_dt = start_dt + pd.Timedelta(days=10)
    end_dt = to_session_label(end_dt)

    canonical_name = resolve_alias(name)
    cal = ec.get_calendar(canonical_name, start=start_dt, end=end_dt, side="right")
    if cache:
        # cache the latest instance
        _cache[canonical_name] = cal
    return cal


def get_calendar(name):
    """The instance cached by the latest ``get_calendar_in_range(..., cache=True)``."""
    try:
        return _cache[resolve_alias(name)]
    except KeyError:
        raise RuntimeError("Calendar instance doesn't exist.")
