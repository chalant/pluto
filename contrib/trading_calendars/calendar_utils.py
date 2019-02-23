from trading_calendars import calendar_utils as cu

import pandas as pd

def get_calendar_in_range(name, start_dt, end_dt=None):
    dis = cu.global_calendar_dispatcher
    canonical_name = dis.resolve_alias(name)
    try:
        factory = dis._calendar_factories[canonical_name]
    except KeyError:
        # We don't have a factory registered for this name.  Barf.
        raise cu.InvalidCalendarName(calendar_name=name)
    if end_dt is None:
        end_dt = start_dt + pd.Timedelta(days=365)
    return factory(start=start_dt,end_dt=end_dt)


