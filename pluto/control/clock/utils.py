import pandas as pd

from pluto.control.clock import sim_engine as sim

def get_generator(calendar, sessions, minute_emission=False, frequency='day'):
    # loops every x frequency

    trading_o_and_c = calendar.schedule.loc[sessions]
    market_closes = trading_o_and_c['market_close']

    if minute_emission:
        market_opens = trading_o_and_c['market_open']
        execution_opens = calendar.execution_time_from_open(market_opens)
        execution_closes = calendar.execution_time_from_close(market_closes)
    else:
        execution_closes = calendar.execution_time_from_close(market_closes)
        execution_opens = execution_closes

    if frequency == 'minute':
        return sim.MinuteSimulationClock(
            sessions,
            execution_opens,
            execution_closes,
            pd.DatetimeIndex([ts - pd.Timedelta(minutes=2) for ts in execution_opens]),
            pd.DatetimeIndex([ts - pd.Timedelta(minutes=15) for ts in execution_closes]),
            minute_emission)
    else:
        return sim.MinuteSimulationClock(
            sessions,
            execution_closes,
            execution_closes,
            pd.DatetimeIndex([ts - pd.Timedelta(minutes=2) for ts in execution_opens]),
            execution_closes,
            minute_emission
        )