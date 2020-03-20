import abc

from pluto.control.loop import simulation_loop
from pluto.control.modes import simulation_mode
from pluto.control import commands
from pluto.data.universes import universes


class RunParameter(object):
    __slots__ = [
        '_session',
        '_capital_ratio',
        '_max_leverage',
        '_start_dt',
        '_end_dt',
        '_platform']

    def __init__(self,
                 session,
                 capital_ratio,
                 max_leverage,
                 start_dt,
                 end_dt,
                 platform):
        '''

        Parameters
        ----------
        session : pluto.interface.directory._Session
        capital_ratio : float
        max_leverage : float
        start_dt : pandas.Timestamp
        end_dt : pandas.Timestamp
        platform : str
        '''
        self._session = session
        self._capital_ratio = capital_ratio
        self._max_leverage = max_leverage
        self._start_dt = start_dt
        self._end_dt = end_dt
        self._platform = platform

    @property
    def session_id(self):
        return self._session.id

    @property
    def strategy_id(self):
        return self._session.strategy_id

    @property
    def session(self):
        return self._session

    @property
    def capital_ratio(self):
        return self._capital_ratio

    @property
    def max_leverage(self):
        return self._max_leverage

    @property
    def start(self):
        return self._start_dt

    @property
    def end(self):
        return self._end_dt

    @property
    def platform(self):
        return self._platform


class Controller(abc.ABC):
    def run(self, directory, params):
        raise NotImplementedError


class SimulationController(Controller):
    def __init__(self,
                 mode,
                 loop,
                 start,
                 end):
        self._start = start
        self._end = end
        self._mode = mode
        self._loop = loop
        super(SimulationController, self).__init__()

    def run(self, directory, params):
        '''

        Parameters
        ----------
        directory: pluto.interface.directory._Read
        params: protos.controller_pb2.RunParams

        '''
        loop = self._loop
        calendars = []
        parameters = []
        start = self._start
        end = self._end

        uni = {}  # universe cache
        for p in params:
            session = directory.get_session(p.session_id)
            uni_name = session.universe_name
            universe = uni.get(uni_name, None)
            if not universe:
                uni[universe] = universe = universes.get_universe(uni_name)
            calendars.extend(universe.calendars)
            parameters.append(
                RunParameter(
                    session,
                    p.capital_ratio,
                    p.max_leverage,
                    start,
                    end,
                    'pluto'))
        loop.execute(commands.Run(directory, parameters, set(calendars)))
        loop.start()
        #todo: if the loop is already running, raise an error? in live,
        # the run method behaves differently (can be called multiple times)

    def stop(self):
        self._loop.stop()


class LiveController(Controller):
    def __init__(self, framework_url):
        super(LiveController, self).__init__()

    def run(self, directory, params):
        parameters = []
        exchanges = []
        for session, capital_ratio, max_leverage in params:
            exchanges.extend(session.exchanges)
            # todo what should be the end date? today? previous open session?
            # the start dt should be : end_dt - session.look_back
            parameters.append(
                RunParameter(
                    session,
                    capital_ratio,
                    max_leverage
                )
            )
