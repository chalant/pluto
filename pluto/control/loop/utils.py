from pluto.control.loop import simulation_loop

class SimpleSimulationLoopFactory(object):
    def get_loop(self, start, end, frequency='day'):
        return simulation_loop.SimulationLoop(start, end, frequency)