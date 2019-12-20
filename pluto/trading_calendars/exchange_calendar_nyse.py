from itertools import chain

from datetime import datetime
from pluto.trading_calendars.calendar_utils import TradingCalendarFactory


from pluto.coms.utils import conversions as crv
from protos import calendar_pb2 as cal
from pluto.trading_calendars import us_holidays as hol


class NYSETradingCalendar(TradingCalendarFactory):
    def _create_calendar(self, start_dt, end_dt):
        """

            Parameters
            ----------
            start_dt : pandas.Timestamp
            end_dt : pandas.Timestamp

            Returns
            -------

            """
        return cal.Calendar(
            start=crv.to_proto_timestamp(start_dt.to_datetime()),
            end=crv.to_proto_timestamp(end_dt.to_datetime()),
            name="NYSE",
            tz='US/Eastern',
            open_time=cal.Time(hour=9, minute=31, second=0),
            close_time=cal.Time(hour=16, minute=0, second=0),
            regular_holidays=[
                hol.USNewYearsDay,
                hol.USMartinLutherKingJrAfter1998,
                hol.USPresidentsDay,
                hol.GoodFriday,
                hol.USMemorialDay,
                hol.USLaborDay,
                hol.USThanksgivingDay,
                hol.Christmas
            ],
            adhoc_holidays=list(chain(
                hol.September11Closings,
                hol.HurricaneSandyClosings,
                hol.USNationalDaysofMourning)),
            special_closes=[
                cal.TimeHolidays(
                    time=cal.Time(hour=13, minute=0, second=0),
                    holidays=[
                        hol.MonTuesThursBeforeIndependenceDay,
                        hol.FridayAfterIndependenceDayExcept2013,
                        hol.USBlackFridayInOrAfter1993,
                        hol.ChristmasEveInOrAfter1993
                    ]),
                cal.TimeHolidays(
                    time=cal.Time(hour=14, minute=0, second=0),
                    holidays=[
                        hol.ChristmasEveBefore1993,
                        hol.USBlackFridayBefore1993
                    ])],
            special_closes_adhoc=[
                cal.TimeDates(
                    time=cal.Time(hour=13, minute=0, second=0),
                    dates=[
                        crv.to_proto_timestamp(datetime(1997, 12, 26)),
                        crv.to_proto_timestamp(datetime(1999, 12, 31)),
                        crv.to_proto_timestamp(datetime(2003, 12, 26)),
                        crv.to_proto_timestamp(datetime(2013, 7, 3))
                    ])
            ],
            open_offset=0,
            execution_time_from_open=cal.TimeDelta(type=cal.TimeDelta.HOURS, value=0),
            execution_time_from_close=cal.TimeDelta(type=cal.TimeDelta.HOURS, value=0)
        )
