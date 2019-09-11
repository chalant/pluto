from trading_calendars import calendar_utils as cu

import itertools as it

from trading_calendars import trading_calendar as tc
import datetime as dt

from dateutil import relativedelta as rd

from contrib.coms.utils import conversions as cvr
from protos import calendar_pb2 as cpb

import pandas as pd
from pandas.tseries import holiday, offsets


class ZiplineCalendarError(Exception):
    msg = None

    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def __str__(self):
        msg = self.msg.format(**self.kwargs)
        return msg

    __unicode__ = __str__
    __repr__ = __str__


class InvalidCalendarName(ZiplineCalendarError):
    """
    Raised when a calendar with an invalid name is requested.
    """
    msg = (
        "The requested TradingCalendar, {calendar_name}, does not exist."
    )


class CalendarNameCollision(ZiplineCalendarError):
    """
    Raised when the static calendar registry already has a calendar with a
    given name.
    """
    msg = (
        "A calendar with the name {calendar_name} is already registered."
    )


class CyclicCalendarAlias(ZiplineCalendarError):
    """
    Raised when calendar aliases form a cycle.
    """
    msg = "Cycle in calendar aliases: [{cycle}]"


class ScheduleFunctionWithoutCalendar(ZiplineCalendarError):
    """
    Raised when schedule_function is called but there is not a calendar to be
    used in the construction of an event rule.
    """
    # TODO update message when new TradingSchedules are built
    msg = (
        "To use schedule_function, the TradingAlgorithm must be running on an "
        "ExchangeTradingSchedule, rather than {schedule}."
    )


class ScheduleFunctionInvalidCalendar(ZiplineCalendarError):
    """
    Raised when schedule_function is called with an invalid calendar argument.
    """
    msg = (
        "Invalid calendar '{given_calendar}' passed to schedule_function. "
        "Allowed options are {allowed_calendars}."
    )


def get_calendar_in_range(name, start_dt, end_dt=None):
    """

    Parameters
    ----------
    name
    start_dt : pandas.Timestamp
    end_dt

    Returns
    -------
    trading_calendars.TradingCalendar
    """
    dis = cu.global_calendar_dispatcher
    canonical_name = dis.resolve_alias(name)
    try:
        factory = dis._calendar_factories[canonical_name]
    except KeyError:
        # We don't have a factory registered for this name.  Barf.
        raise cu.InvalidCalendarName(calendar_name=name)
    if end_dt is None:
        end_dt = start_dt + pd.Timedelta(days=10)
    return factory(start=start_dt, end=end_dt)



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
        self._calendar_factories = calendar_factories
        self._aliases = aliases

    def get_calendar(self, name, start_dt, end_dt):
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
        # Use an OrderedDict as an ordered set so that we can return the order
        # of aliases in the event of a cycle.
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


class TradingCalendar(tc.TradingCalendar):
    def __init__(self, start, end, proto_calendar):
        super(TradingCalendar, self).__init__(start, end)
        self._proto_calendar = proto_calendar
        self._regular_early_close = self._from_proto_time(proto_calendar.regular_early_close)

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
        return [(sp.time,
                 tc.HolidayCalendar([self._from_proto_holiday(hol) for hol in sp.holidays]))
                for sp in self._proto_calendar.special_closes]

    def special_closes_adhoc(self):
        return [(sp.time,
                 [cvr.to_datetime(t).strftime("%Y-%m-%d") for t in sp.dates])
                for sp in self._proto_calendar.special_closes_adhoc]

def from_proto_calendar(proto_calendar, start, end=None):
    if end is None:
        end = start + pd.Timedelta(days=365)
    return TradingCalendar(start, end, proto_calendar)