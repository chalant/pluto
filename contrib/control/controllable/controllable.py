from abc import ABC, abstractmethod

from contrib.control.controllable import control

class Controllable(ABC):
    def __init__(self):
        self._algo_controller = None
        self._metrics_tracker = None
        self._bundler = None
        self._calendar = None

        self._state_storage_path = None
        self._broker = None


    def initialize(self, dt, calendar, strategy, capital, max_leverage, benchmark_asset, restrictions):
        controller = self._algo_controller
        if not controller:
            self._algo_controller = controller = control.AlgorithmController(strategy, benchmark_asset, restrictions)
        controller.on_initialize(dt, self._metrics_tracker, self._bundler, calendar)

    def session_start(self, dt):
        pass

    def bar(self, dt):
        pass

    def session_end(self, dt):
        return

    def stop(self, dt):
        pass

    def liquidate(self, dt):
        pass

    def update_parameters(self, capital, max_leverage):
        pass







