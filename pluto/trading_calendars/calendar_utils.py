import itertools as it

import datetime as dt

from dateutil import relativedelta as rd

from trading_calendars import trading_calendar as tc
from trading_calendars import calendar_utils as cu

import pandas as pd
from pandas.tseries import holiday, offsets

from protos import calendar_pb2 as cpb

from pluto.coms.utils import conversions as cvr
from pluto.trading_calendars import wrappers as wr

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
    return wr.OpenOffsetFix(start_dt, end_dt, factory)


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

def from_proto_calendar(proto_calendar, start, end=None):
    if end is None:
        end = start + pd.Timedelta(days=365)
    return TradingCalendar(start, end, proto_calendar)

def to_proto_calendar(calendar):
    #todo
    pass