import itertools as it

import datetime as dt

from dateutil import relativedelta as rd

from trading_calendars import trading_calendar as tc
from trading_calendars.errors import (
    CalendarNameCollision,
    CyclicCalendarAlias,
    InvalidCalendarName
)
from trading_calendars.always_open import AlwaysOpenCalendar
from trading_calendars.exchange_calendar_asex import ASEXExchangeCalendar
from trading_calendars.exchange_calendar_bvmf import BVMFExchangeCalendar
from trading_calendars.exchange_calendar_cmes import CMESExchangeCalendar
from trading_calendars.exchange_calendar_iepa import IEPAExchangeCalendar
from trading_calendars.exchange_calendar_xams import XAMSExchangeCalendar
from trading_calendars.exchange_calendar_xasx import XASXExchangeCalendar
from trading_calendars.exchange_calendar_xbkk import XBKKExchangeCalendar
from trading_calendars.exchange_calendar_xbog import XBOGExchangeCalendar
from trading_calendars.exchange_calendar_xbom import XBOMExchangeCalendar
from trading_calendars.exchange_calendar_xbru import XBRUExchangeCalendar
from trading_calendars.exchange_calendar_xbud import XBUDExchangeCalendar
from trading_calendars.exchange_calendar_xbue import XBUEExchangeCalendar
from trading_calendars.exchange_calendar_xcbf import XCBFExchangeCalendar
from trading_calendars.exchange_calendar_xcse import XCSEExchangeCalendar
from trading_calendars.exchange_calendar_xdub import XDUBExchangeCalendar
from trading_calendars.exchange_calendar_xfra import XFRAExchangeCalendar
from trading_calendars.exchange_calendar_xhel import XHELExchangeCalendar
from trading_calendars.exchange_calendar_xhkg import XHKGExchangeCalendar
from trading_calendars.exchange_calendar_xice import XICEExchangeCalendar
from trading_calendars.exchange_calendar_xidx import XIDXExchangeCalendar
from trading_calendars.exchange_calendar_xist import XISTExchangeCalendar
from trading_calendars.exchange_calendar_xjse import XJSEExchangeCalendar
from trading_calendars.exchange_calendar_xkar import XKARExchangeCalendar
from trading_calendars.exchange_calendar_xkls import XKLSExchangeCalendar
from trading_calendars.exchange_calendar_xkrx import XKRXExchangeCalendar
from trading_calendars.exchange_calendar_xlim import XLIMExchangeCalendar
from trading_calendars.exchange_calendar_xlis import XLISExchangeCalendar
from trading_calendars.exchange_calendar_xlon import XLONExchangeCalendar
from trading_calendars.exchange_calendar_xmad import XMADExchangeCalendar
from trading_calendars.exchange_calendar_xmex import XMEXExchangeCalendar
from trading_calendars.exchange_calendar_xmil import XMILExchangeCalendar
from trading_calendars.exchange_calendar_xmos import XMOSExchangeCalendar
from trading_calendars.exchange_calendar_xnys import XNYSExchangeCalendar
from trading_calendars.exchange_calendar_xnze import XNZEExchangeCalendar
from trading_calendars.exchange_calendar_xosl import XOSLExchangeCalendar
from trading_calendars.exchange_calendar_xpar import XPARExchangeCalendar
from trading_calendars.exchange_calendar_xphs import XPHSExchangeCalendar
from trading_calendars.exchange_calendar_xpra import XPRAExchangeCalendar
from trading_calendars.exchange_calendar_xses import XSESExchangeCalendar
from trading_calendars.exchange_calendar_xsgo import XSGOExchangeCalendar
from trading_calendars.exchange_calendar_xshg import XSHGExchangeCalendar
from trading_calendars.exchange_calendar_xsto import XSTOExchangeCalendar
from trading_calendars.exchange_calendar_xswx import XSWXExchangeCalendar
from trading_calendars.exchange_calendar_xtai import XTAIExchangeCalendar
from trading_calendars.exchange_calendar_xtks import XTKSExchangeCalendar
from trading_calendars.exchange_calendar_xtse import XTSEExchangeCalendar
from trading_calendars.exchange_calendar_xwar import XWARExchangeCalendar
from trading_calendars.exchange_calendar_xwbo import XWBOExchangeCalendar
from trading_calendars.us_futures_calendar import QuantopianUSFuturesCalendar
from trading_calendars.weekday_calendar import WeekdayCalendar

import pandas as pd
from pandas.tseries import holiday, offsets

from pluto.coms.utils import conversions as cvr
from pluto.trading_calendars import wrappers as wr

from protos import calendar_pb2 as cpb

_default_calendar_factories = {
    # Exchange calendars.
    'ASEX': ASEXExchangeCalendar,
    'BVMF': BVMFExchangeCalendar,
    'CMES': CMESExchangeCalendar,
    'IEPA': IEPAExchangeCalendar,
    'XAMS': XAMSExchangeCalendar,
    'XASX': XASXExchangeCalendar,
    'XBKK': XBKKExchangeCalendar,
    'XBOG': XBOGExchangeCalendar,
    'XBOM': XBOMExchangeCalendar,
    'XBRU': XBRUExchangeCalendar,
    'XBUD': XBUDExchangeCalendar,
    'XBUE': XBUEExchangeCalendar,
    'XCBF': XCBFExchangeCalendar,
    'XCSE': XCSEExchangeCalendar,
    'XDUB': XDUBExchangeCalendar,
    'XFRA': XFRAExchangeCalendar,
    'XHEL': XHELExchangeCalendar,
    'XHKG': XHKGExchangeCalendar,
    'XICE': XICEExchangeCalendar,
    'XIDX': XIDXExchangeCalendar,
    'XIST': XISTExchangeCalendar,
    'XJSE': XJSEExchangeCalendar,
    'XKAR': XKARExchangeCalendar,
    'XKLS': XKLSExchangeCalendar,
    'XKRX': XKRXExchangeCalendar,
    'XLIM': XLIMExchangeCalendar,
    'XLIS': XLISExchangeCalendar,
    'XLON': XLONExchangeCalendar,
    'XMAD': XMADExchangeCalendar,
    'XMEX': XMEXExchangeCalendar,
    'XMIL': XMILExchangeCalendar,
    'XMOS': XMOSExchangeCalendar,
    'XNYS': XNYSExchangeCalendar,
    'XNZE': XNZEExchangeCalendar,
    'XOSL': XOSLExchangeCalendar,
    'XPAR': XPARExchangeCalendar,
    'XPHS': XPHSExchangeCalendar,
    'XPRA': XPRAExchangeCalendar,
    'XSES': XSESExchangeCalendar,
    'XSGO': XSGOExchangeCalendar,
    'XSHG': XSHGExchangeCalendar,
    'XSTO': XSTOExchangeCalendar,
    'XSWX': XSWXExchangeCalendar,
    'XTAI': XTAIExchangeCalendar,
    'XTKS': XTKSExchangeCalendar,
    'XTSE': XTSEExchangeCalendar,
    'XWAR': XWARExchangeCalendar,
    'XWBO': XWBOExchangeCalendar,
    # Miscellaneous calendars.
    'us_futures': QuantopianUSFuturesCalendar,
    '24/7': AlwaysOpenCalendar,
    '24/5': WeekdayCalendar,
}
_default_calendar_aliases = {
    'AMEX': 'XNYS',
    'NYSE': 'XNYS',
    'NASDAQ': 'XNYS',
    'BATS': 'XNYS',
    'FWB': 'XFRA',
    'LSE': 'XLON',
    'TSX': 'XTSE',
    'BMF': 'BVMF',
    'CME': 'CMES',
    'CBOT': 'CMES',
    'COMEX': 'CMES',
    'NYMEX': 'CMES',
    'ICE': 'IEPA',
    'ICEUS': 'IEPA',
    'NYFE': 'IEPA',
    'CFE': 'XCBF',
    'JKT': 'XIDX',
}

class TradingCalendarDispatcher(object):
    """
    A class for dispatching and caching trading calendars.

    Methods of a global instance of this class are provided by
    calendars.calendar_utils.

    Parameters
    ----------
    calendars : dict[str -> TradingCalendar]
        Initial set of calendars.
    calendar_factories : dict[str -> function]
        Factories for lazy calendar creation.
    aliases : dict[str -> str]
        Calendar name aliases.
    """
    def __init__(self, calendars, calendar_factories, aliases):
        self._calendars = calendars
        self._calendar_factories = dict(calendar_factories)
        self._aliases = dict(aliases)

    def get_calendar(self, name):
        """
        Retrieves an instance of an TradingCalendar whose name is given.

        Parameters
        ----------
        name : str
            The name of the TradingCalendar to be retrieved.

        Returns
        -------
        calendar : calendars.TradingCalendar
            The desired calendar.
        """
        canonical_name = self.resolve_alias(name)

        try:
            return self._calendars[canonical_name]
        except KeyError:
            # We haven't loaded this calendar yet, so make a new one.
            pass

        try:
            factory = self._calendar_factories[canonical_name]
        except KeyError:
            # We don't have a factory registered for this name.  Barf.
            raise InvalidCalendarName(calendar_name=name)

        # Cache the calendar for future use.
        calendar = self._calendars[canonical_name] = factory()
        return calendar

    def has_calendar(self, name):
        """
        Do we have (or have the ability to make) a calendar with ``name``?
        """
        return (
            name in self._calendars
            or name in self._calendar_factories
            or name in self._aliases
        )

    def register_calendar(self, name, calendar, force=False):
        """
        Registers a calendar for retrieval by the get_calendar method.

        Parameters
        ----------
        name: str
            The key with which to register this calendar.
        calendar: TradingCalendar
            The calendar to be registered for retrieval.
        force : bool, optional
            If True, old calendars will be overwritten on a name collision.
            If False, name collisions will raise an exception.
            Default is False.

        Raises
        ------
        CalendarNameCollision
            If a calendar is already registered with the given calendar's name.
        """
        if force:
            self.deregister_calendar(name)

        if self.has_calendar(name):
            raise CalendarNameCollision(calendar_name=name)

        self._calendars[name] = calendar

    def register_calendar_type(self, name, calendar_type, force=False):
        """
        Registers a calendar by type.

        This is useful for registering a new calendar to be lazily instantiated
        at some future point in time.

        Parameters
        ----------
        name: str
            The key with which to register this calendar.
        calendar_type: type
            The type of the calendar to register.
        force : bool, optional
            If True, old calendars will be overwritten on a name collision.
            If False, name collisions will raise an exception.
            Default is False.

        Raises
        ------
        CalendarNameCollision
            If a calendar is already registered with the given calendar's name.
        """
        if force:
            self.deregister_calendar(name)

        if self.has_calendar(name):
            raise CalendarNameCollision(calendar_name=name)

        self._calendar_factories[name] = calendar_type

    def register_calendar_alias(self, alias, real_name, force=False):
        """
        Register an alias for a calendar.

        This is useful when multiple exchanges should share a calendar, or when
        there are multiple ways to refer to the same exchange.

        After calling ``register_alias('alias', 'real_name')``, subsequent
        calls to ``get_calendar('alias')`` will return the same result as
        ``get_calendar('real_name')``.

        Parameters
        ----------
        alias : str
            The name to be used to refer to a calendar.
        real_name : str
            The canonical name of the registered calendar.
        force : bool, optional
            If True, old calendars will be overwritten on a name collision.
            If False, name collisions will raise an exception.
            Default is False.
        """
        if force:
            self.deregister_calendar(alias)

        if self.has_calendar(alias):
            raise CalendarNameCollision(calendar_name=alias)

        self._aliases[alias] = real_name

        # Ensure that the new alias doesn't create a cycle, and back it out if
        # we did.
        try:
            self.resolve_alias(alias)
        except CyclicCalendarAlias:
            del self._aliases[alias]
            raise

    def resolve_alias(self, name):
        """
        Resolve a calendar alias for retrieval.

        Parameters
        ----------
        name : str
            The name of the requested calendar.

        Returns
        -------
        canonical_name : str
            The real name of the calendar to create/return.
        """
        seen = []

        while name in self._aliases:
            seen.append(name)
            name = self._aliases[name]

            # This is O(N ** 2), but if there's an alias chain longer than 2,
            # something strange has happened.
            if name in seen:
                seen.append(name)
                raise CyclicCalendarAlias(
                    cycle=" -> ".join(repr(k) for k in seen)
                )

        return name

    def deregister_calendar(self, name):
        """
        If a calendar is registered with the given name, it is de-registered.

        Parameters
        ----------
        cal_name : str
            The name of the calendar to be deregistered.
        """
        self._calendars.pop(name, None)
        self._calendar_factories.pop(name, None)
        self._aliases.pop(name, None)

    def clear_calendars(self):
        """
        Deregisters all current registered calendars
        """
        self._calendars.clear()
        self._calendar_factories.clear()
        self._aliases.clear()

global_calendar_dispatcher = TradingCalendarDispatcher(
    calendars={},
    calendar_factories=_default_calendar_factories,
    aliases=_default_calendar_aliases
)

_cache = {}

def resolve_alias(name):
    return global_calendar_dispatcher.resolve_alias(name)

def get_calendar_in_range(name, start_dt, end_dt=None, cache=False):
    """

    Parameters
    ----------
    name: str
    start_dt: pandas.Timestamp
    end_dt: pandas.Timestamp

    Returns
    -------
    trading_calendars.TradingCalendar
    """

    dis = global_calendar_dispatcher
    try:
        factory = dis._calendar_factories[name]
    except KeyError:
        # We don't have a factory registered for this name.  Barf.
        raise InvalidCalendarName(calendar_name=name)
    if end_dt is None:
        end_dt = start_dt + pd.Timedelta(days=10)
    cal = wr.OpenOffsetFix(start_dt, end_dt, factory)
    if cache:
        #cache the latest instance
        _cache[name] = cal
    return cal

def get_calendar(name):
    try:
        cal = _cache[name]
        return cal
    except KeyError:
        raise RuntimeError("Calendar instance doesn't exist.")

class TradingCalendar(tc.TradingCalendar):
    #todo: instead of calling init, we should call __new__
    def __init__(self, start, end, proto_calendar):
        super(TradingCalendar, self).__init__(start, end)
        self._proto_calendar = proto_calendar
        self._regular_early_close = self._from_proto_time(proto_calendar.regular_early_close)

    def __new__(cls, start, end, proto_calendar):
        pass

    def _from_proto_time(self, proto_time):
        return dt.time(proto_time.hour, proto_time.minute)

    def _from_proto_holiday(self, proto_holiday):
        offsts = [self._from_proto_offset(offset) for offset in proto_holiday.offsets]
        return holiday.Holiday(
            proto_holiday.name,
            proto_holiday.year,
            proto_holiday.month,
            proto_holiday.day,
            offset=offsts.pop() if len(offsts) == 1 else offsts,
            observance=self._from_proto_observance(proto_holiday.observance),
            start_date=pd.Timestamp(cvr.to_datetime(proto_holiday.start_date)),
            end_date=pd.Timestamp(cvr.to_datetime(proto_holiday.end_date)),
            days_of_week=[day for day in proto_holiday.days_of_week]
        )

    def _from_proto_offset(self, proto_offset):
        if proto_offset.type == cpb.Offset.MONDAY:
            return pd.DateOffset(weekday=rd.MO(proto_offset.n))
        elif proto_offset.type == cpb.Offset.DAY:
            return pd.DateOffset(offsets.Day(proto_offset.n))
        elif proto_offset.type == cpb.Offset.THURSDAY:
            return pd.DateOffset(weekday=rd.TH(proto_offset.n))
        elif proto_offset.type == cpb.Offset.EASTER:
            return pd.DateOffset(offsets.Easter())
        else:
            return

    def _from_proto_observance(self, proto_observance):
        if proto_observance == cpb.Observance.NEAREST_WORKDAY:
            return holiday.nearest_workday
        elif proto_observance == cpb.Observance.SUNDAY_TO_MONDAY:
            return holiday.sunday_to_monday
        else:
            return

    def close_times(self):
        pass

    def open_times(self):
        pass

    def name(self):
        return self._proto_calendar.name

    def tz(self):
        return self._proto_calendar.timezone

    def open_time(self):
        return self._from_proto_time(self._proto_calendar.open_time)

    def close_time(self):
        return self._from_proto_time(self._proto_calendar.close_time)

    def regular_holidays(self):
        return tc.HolidayCalendar(
            [self._from_proto_holiday(hol) for hol in self._proto_calendar.regular_holidays]
        )

    def adhoc_holidays(self):
        return list(
            it.chain(
                *[pd.date_range(
                    cvr.to_datetime(r.start_date),
                    cvr.to_datetime(r.end_date))
                    for r in self._proto_calendar.adhoc_holidays])
        )

    def special_closes(self):
        return [
            (sp.time,
             tc.HolidayCalendar(
                 [self._from_proto_holiday(hol)
                  for hol in sp.holidays]))
            for sp in self._proto_calendar.special_closes]

    def special_closes_adhoc(self):
        return [
            (sp.time,
             [cvr.to_datetime(t).strftime("%Y-%m-%d")
              for t in sp.dates])
            for sp in self._proto_calendar.special_closes_adhoc]

def from_proto_calendar(proto_calendar, start, end=None, cache=False):
    if end is None:
        end = start + pd.Timedelta(days=365)
    #todo: cache the calendar
    return TradingCalendar(start, end, proto_calendar)

def to_proto_calendar(calendar):
    #todo
    pass