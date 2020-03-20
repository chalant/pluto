from abc import ABC, abstractmethod


class Command(ABC):
    @abstractmethod
    def __call__(self, control_mode, clock_factory):
        '''

        Parameters
        ----------
        control_mode: pluto.control.modes.mode.ControlMode
        clock_factory: function

        Returns
        -------

        '''
        raise NotImplementedError


class Stop(Command):
    def __init__(self, params, liquidate=False):
        self._liquidate = liquidate
        self._params = params

    def __call__(self, control_mode, clock_factory):
        control_mode.stop(self._params)


class Run(Command):
    def __init__(self, directory, params, exchanges):
        self._params = params
        self._exchanges = exchanges
        self._directory = directory

    def __call__(self, control_mode, clock_factory):
        clock_factory(self._exchanges)
        control_mode.add_strategies(self._directory, self._params)
