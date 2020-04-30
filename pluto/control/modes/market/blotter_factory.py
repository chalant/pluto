import abc

from pluto.finance.blotter import simulation_blotter
from pluto import setup

class SimulationBlotterFactory(abc.ABC):
    @property
    @abc.abstractmethod
    def blotters(self):
        raise NotImplementedError

    @abc.abstractmethod
    def add_blotter(self, session_id, universe):
        raise NotImplementedError

    @abc.abstractmethod
    def get_blotter(self, session_id):
        raise NotImplementedError

class SingleSimulationBlotterFactory(SimulationBlotterFactory):
    def __init__(self, cancel_policy=None):
        stp = setup.load_setup()
        self._blotter = simulation_blotter.SimulationBlotter(
            commission_models=stp.get_slippage_models(),
            slippage_models=stp.get_slippage_models(),
            cancel_policy=cancel_policy)

    @property
    def blotters(self):
        yield self._blotter

    def add_blotter(self, session_id, universe):
        return self._blotter

    def get_blotter(self, session_id):
        return self._blotter

class MultiSimulationBlotterFactory(SimulationBlotterFactory):
    def __init__(self, cancel_policy=None):
        self._blotters = {}
        self._cancel_policy = cancel_policy

    @property
    def blotters(self):
        for blotter in self._blotters.values():
            yield blotter

    def add_blotter(self, session_id, universe):
        stp = setup.load_setup(universe)
        blotter = self._blotters.get(session_id, None)
        if not blotter:
            self._blotters[session_id] = \
                simulation_blotter.SimulationBlotter(
                commission_models=stp.get_commission_models(),
                slippage_models=stp.get_slippage_models(),
                cancel_policy=self._cancel_policy
            )

    def get_blotter(self, session_id):
        return self._blotters[session_id]