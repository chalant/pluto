from pluto.control.modes.market import simulation_blotter

class SingleSimulationBlotterFactory(object):
    def __init__(self,
                 equity_slippage=None,
                 future_slippage=None,
                 equity_commission=None,
                 future_commission=None,
                 cancel_policy=None):
        self._blotter = simulation_blotter.SimulationBlotter(
            equity_slippage,
            future_slippage,
            equity_commission,
            future_commission,
            cancel_policy)

    @property
    def blotters(self):
        yield self._blotter

    def add_blotter(self, session_id):
        return self._blotter

class MultiSimulationBlotterFactory(object):
    def __init__(self,
                 equity_slippage_fct,
                 future_slippage_fct,
                 equity_commission_fct,
                 future_commission_fct,
                 ca):
        self._blotters = {}

    @property
    def blotters(self):
        for blotter in self._blotters.values():
            yield blotter

    def add_blotter(self, session_id):
        blotter = self._blotters.get(session_id, None)
        if not blotter:
            self._blotters[session_id] = simulation_blotter.SimulationBlotter()