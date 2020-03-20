import pandas as pd

from pluto.trading_calendars.protos import calendar_pb2 as cal
from pluto.coms.utils import conversions as crv

MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY, SATURDAY, SUNDAY = range(7)


def to_proto_timestamp(ts):
    return crv.to_proto_timestamp(ts.to_datetime())


USLaborDay = cal.Holiday(
    name='Labor Day',
    month=9,
    day=1,
    offset=[cal.Offset(type=cal.Offset.MONDAY, n=1)]
)

USColumbusDay = cal.Holiday(
    name='Columbus Day',
    month=10,
    day=1,
    offset=[cal.Offset(type=cal.Offset.MONDAY, n=2)]
)

USThanksgivingDay = cal.Holiday(
    name='Thanksgiving',
    month=11,
    day=1,
    offset=cal.Offset(type=cal.Offset.THURSDAY, n=4)
)

USMartinLutherKingJr = cal.Holiday(
    name='Dr. Martin Luther King Jr.',
    start_date=to_proto_timestamp(pd.Timestamp('1986-01-01')),
    month=1,
    day=1,
    offset=[cal.Offset(type=cal.Offset.MONDAY, n=2)]
)

USPresidentsDay = cal.Holiday(
    name="President's Day",
    month=2,
    day=1,
    offset=[cal.Offset(type=cal.Offset.MONDAY, n=3)]
)

GoodFriday = cal.Holiday(
    name="Good Friday",
    month=1,
    day=1,
    offset=[cal.Offset(type=cal.Offset.EASTER), cal.Offset(type=cal.Offset.DAY, n=-2)]
)

EasterMonday = cal.Holiday(
    name="Easter Monday",
    month=1,
    day=1,
    offset=[cal.Offset(type=cal.Offset.EASTER), cal.Offset(type=cal.Offset.DAY, n=1)]
)

ChristmasEveBefore1993 = cal.Holiday(
    name='Christmas Eve',
    month=12,
    day=24,
    end_date=to_proto_timestamp(pd.Timestamp('1993-01-01')),
    days_of_week=(MONDAY, TUESDAY, WEDNESDAY, THURSDAY)
)

ChristmasEveInOrAfter1993 = cal.Holiday(
    name='Christmas Eve',
    month=12,
    day=24,
    end_date=to_proto_timestamp(pd.Timestamp('1993-01-01'))
)

USNewYearsDay = cal.Holiday(
    name='New Years Day',
    month=1,
    day=1,
    observance=cal.SUNDAY_TO_MONDAY
)

USMartinLutherKingJrAfter1998 = cal.Holiday(
    name='Dr. Martin Luther King Jr. Day',
    month=1,
    day=1,
    start_date=to_proto_timestamp(pd.Timestamp('1998-01-01')),
    offset=[cal.Offset(type=cal.Offset.MONDAY, n=3)]
)

USMemorialDay = cal.Holiday(
    name='Memorial Day',
    month=5,
    day=25,
    offset=[cal.Offset(type=cal.Offset.MONDAY, n=1)]
)

USIndependenceDay = cal.Holiday(
    name='July 4th',
    month=7,
    day=4,
    observance=cal.NEAREST_WORKDAY
)

Christmas = cal.Holiday(
    name='Christmas',
    month=12,
    day=25,
    observance=cal.NEAREST_WORKDAY
)

MonTuesThursBeforeIndependenceDay = cal.Holiday(
    # When July 4th is a Tuesday, Wednesday, or Friday, the previous day is a
    # half day.
    name='Mondays, Tuesdays, and Thursdays Before Independence Day',
    month=7,
    day=3,
    days_of_week=(MONDAY, TUESDAY, THURSDAY),
    start_date=to_proto_timestamp(pd.Timestamp("1995-01-01")),
)

FridayAfterIndependenceDayExcept2013 = cal.Holiday(
    # When July 4th is a Thursday, the next day is a half day (except in 2013,
    # when, for no explicable reason, Wednesday was a half day instead).
    name="Fridays after Independence Day that aren't in 2013",
    month=7,
    day=5,
    days_of_week=(FRIDAY,),
    observance=cal.JULY_5TH_HOLIDAY_OBSERVANCE,
    start_date=to_proto_timestamp(pd.Timestamp("1995-01-01")),
)

USBlackFridayBefore1993 = cal.Holiday(
    name='Black Friday',
    month=11,
    day=1,
    # Black Friday was not observed until 1992.
    start_date=to_proto_timestamp(pd.Timestamp('1992-01-01')),
    end_date=to_proto_timestamp(pd.Timestamp('1993-01-01')),
    offset=[cal.Offset(type=cal.Offset.THURDAY, n=4), cal.Offset(type=cal.Offset.DAY, n=1)],
)

USBlackFridayInOrAfter1993 = cal.Holiday(
    name='Black Friday',
    month=11,
    day=1,
    start_date=to_proto_timestamp(pd.Timestamp('1993-01-01')),
    offset=[cal.Offset(type=cal.Offset.THURDAY, n=4), cal.Offset(type=cal.Offset.DAY, n=1)],
)

BattleOfGettysburg = cal.Holiday(
    # All of the floor traders in Chicago were sent to PA
    name='Markets were closed during the battle of Gettysburg',
    month=7,
    day=(1, 2, 3),
    start_date=to_proto_timestamp(pd.Timestamp("1863-07-01")),
    end_date=to_proto_timestamp(pd.Timestamp("1863-07-03"))
)

# http://en.wikipedia.org/wiki/Aftermath_of_the_September_11_attacks
September11Closings = cal.DateRange(
    start_date=to_proto_timestamp(pd.Timestamp('2001-09-11')),
    end_date=to_proto_timestamp(pd.Timestamp('2001-09-16')),
    tz='UTC'
)

# http://en.wikipedia.org/wiki/Hurricane_sandy
HurricaneSandyClosings = cal.DateRange(
    start_date=to_proto_timestamp(pd.Timestamp('2012-10-29')),
    end_date=to_proto_timestamp(pd.Timestamp('2012-10-30')),
    tz='UTC'
)

# National Days of Mourning
# - President Richard Nixon - April 27, 1994
# - President Ronald W. Reagan - June 11, 2004
# - President Gerald R. Ford - Jan 2, 2007
USNationalDaysofMourning = [
    to_proto_timestamp(pd.Timestamp('1994-04-27')),
    to_proto_timestamp(pd.Timestamp('2004-06-11')),
    to_proto_timestamp(pd.Timestamp('2007-01-02')),
]
