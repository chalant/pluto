import abc

from pluto.control.modes import simulation_mode, live_simulation_mode

class ModeFactory(abc.ABC):
    def __init__(self, framework_url):
        self._framework_url = framework_url

    @property
    @abc.abstractmethod
    def mode_type(self):
        raise NotImplementedError

    def get_mode(self, capital, max_leverage, process_factory):
        return self._get_mode(
            self._framework_url,
            capital,
            max_leverage,
            process_factory)

    @abc.abstractmethod
    def _get_mode(self, framework_url, capital, max_leverage, process_factory):
        raise NotImplementedError

class SimulationModeFactory(ModeFactory):
    @property
    def mode_type(self):
        return 'simulation'

    def _get_mode(self, framework_url, capital, max_leverage, process_factory):
        return simulation_mode.SimulationControlMode(
            framework_url,
            capital,
            max_leverage,
            process_factory)

class LiveSimulationModeFactory(ModeFactory):
    def __init__(self, framework_url, market_factory):
        super(LiveSimulationModeFactory, self).__init__(framework_url)
        self._market_factory = market_factory

    @property
    def mode_type(self):
        return 'live'

    def _get_mode(self, framework_url, capital, max_leverage, process_factory):
        return live_simulation_mode.LiveSimulationMode(
            framework_url,
            capital,
            max_leverage,
            process_factory,
            self._market_factory)