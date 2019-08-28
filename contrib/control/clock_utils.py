from contrib.control.clock import clock_engine


def create_clock(calendar, start_dt, end_dt=None, emission_rate='daily', type_='simulation'):
    if type_ == 'simulation':
        return clock_engine.SimulationClock(calendar, emission_rate, start_dt, end_dt)