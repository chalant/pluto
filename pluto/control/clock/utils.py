import pandas as pd

from pluto.control.clock import sim_engine as sim


def execution_times(calendar, sessions):
    """Execution opens and closes of ``sessions``.

    As zipline-reloaded's TradingAlgorithm._create_clock: the first minute of a
    session (right-sided calendar: open + 1 minute) and its close; a calendar
    that defines execution times (us_futures) maps them through them.
    """
    market_opens = calendar.first_minutes.loc[sessions]
    market_closes = calendar.schedule.loc[sessions, 'close']

    if hasattr(calendar, 'execution_time_from_open'):
        market_opens = calendar.execution_time_from_open(market_opens)
        market_closes = calendar.execution_time_from_close(market_closes)

    return market_opens, market_closes


def get_generator(calendar, sessions, minute_emission=False, frequency='day'):
    # loops every x frequency

    market_opens, market_closes = execution_times(calendar, sessions)

    execution_closes = market_closes
    if minute_emission:
        execution_opens = market_opens
    else:
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
