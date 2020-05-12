import abc

from pluto.control.modes import simulation_mode, live_simulation_mode

class ModeFactory(abc.ABC):
    def __init__(self, thread_pool, framework_url):
        self._framework_url = framework_url
        self._thread_pool = thread_pool

    @property
    @abc.abstractmethod
    def mode_type(self):
        raise NotImplementedError

    def get_mode(self, capital, max_leverage, process_factory):
        return self._get_mode(
            self._thread_pool,
            self._framework_url,
            capital,
            max_leverage,
            process_factory)

    @abc.abstractmethod
    def _get_mode(self, thread_pool, framework_url, capital, max_leverage, process_factory):
        raise NotImplementedError

class SimulationModeFactory(ModeFactory):
    @property
    def mode_type(self):
        return 'simulation'

    def _get_mode(self, thread_pool, framework_url, capital, max_leverage, process_factory):
        return simulation_mode.SimulationControlMode(
            framework_url,
            capital,
            max_leverage,
            process_factory,
            thread_pool
        )

class LiveSimulationModeFactory(ModeFactory):
    def __init__(self, framework_url, market_factory, thread_pool):
        super(LiveSimulationModeFactory, self).__init__(thread_pool, framework_url)
        self._market_factory = market_factory

    @property
    def mode_type(self):
        return 'live'

    def _get_mode(self, thread_pool, framework_url, capital, max_leverage, process_factory):
        return live_simulation_mode.LiveSimulationMode(
            framework_url,
            capital,
            max_leverage,
            process_factory,
            self._market_factory,
            thread_pool
        )