import abc

from pluto.control.modes import simulation_mode, live_simulation_mode

class ModeFactory(abc.ABC):
    def __init__(self, framework_url, server):
        self._framework_url = framework_url
        self._server = server

    def get_mode(self, capital, max_leverage, process_factory):
        return self._get_mode(
            self._server,
            self._framework_url,
            capital,
            max_leverage,
            process_factory)

    @abc.abstractmethod
    def _get_mode(self, server, framework_url, capital, max_leverage, process_factory):
        raise NotImplementedError

class SimulationModeFactory(ModeFactory):
    def _get_mode(self, server, framework_url, capital, max_leverage, process_factory):
        return simulation_mode.SimulationControlMode(
            server,
            capital,
            max_leverage,
            process_factory)

class LiveSimulationModeFactory(ModeFactory):
    def __init__(self, framework_url, server, market_factory):
        super(LiveSimulationModeFactory, self).__init__(framework_url, server)
        self._market_factory = market_factory

    def _get_mode(self, server, framework_url, capital, max_leverage, process_factory):
        return live_simulation_mode.LiveSimulationMode(
            server,
            capital,
            max_leverage,
            process_factory,
            self._market_factory
        )