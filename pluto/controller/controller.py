from pluto.control import commands
from pluto.data.universes import universes


class _RunParameter(object):
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


class Controller(object):
    def __init__(self, loop, end_dt):
        self._loop = loop
        self._end_dt = end_dt

    def run(self, mode_type, directory, params):
        '''

        Parameters
        ----------
        directory: pluto.interface.directory._Read
        params: protos.controller_pb2.RunParams

        '''
        loop = self._loop
        # start is the same in both loop and controllable
        start = loop.start_dt
        end = self._end_dt

        loop.execute(commands.Run(
            directory,
            loop.get_mode(mode_type),
            params,
            start,
            end))

        loop.start()
        # todo: if the loop is already running, raise an error? in live,
        # the run method behaves differently (can be called multiple times)

    def stop(self):
        self._loop.stop()
