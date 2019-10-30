from datetime import datetime, timedelta

import contrib.control.clock.sim_engine as sim

from trading_calendars.utils.pandas_utils import days_at_time
import pandas as pd



def get_event_generator(calendar, start_dt, end_dt, before_trading_starts_time=None, minute_emission=False):
    # loops every x frequency

    sessions = calendar.sessions_in_range(start_dt, end_dt)
    trading_o_and_c = calendar.schedule.loc[sessions]
    market_closes = trading_o_and_c['market_close']

    dt = datetime.combine(datetime.min, calendar.open_times[0][1])
    dt = dt - pd.Timedelta(minutes=5)
    bfs = dt.time()

    bts_minutes = days_at_time(
        sessions,
        bfs,
        calendar.tz
    )

    # bts_minutes = pd.DatetimeIndex([o - pd.Timedelta(minutes=15) for o in calendar.opens]).sort_values()

    if minute_emission:
        market_opens = trading_o_and_c['market_open']
        execution_opens = calendar.execution_time_from_open(market_opens)
        execution_closes = calendar.execution_time_from_close(market_closes)
    else:
        execution_closes = calendar.execution_time_from_close(market_closes)
        execution_opens = execution_closes
    
    return sim.MinuteSimulationClock(sessions, execution_opens, execution_closes, bts_minutes, minute_emission)