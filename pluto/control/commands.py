from abc import ABC, abstractmethod


class Command(ABC):
    @abstractmethod
    def __call__(self, clock_factory):
        '''

        Parameters
        ----------
        clock_factory: function

        Returns
        -------

        '''
        raise NotImplementedError


class Stop(Command):
    def __init__(self, control_mode, params, liquidate=False):
        self._liquidate = liquidate
        self._params = params
        self._control_mode = control_mode

    def __call__(self, clock_factory):
        self._control_mode.stop(self._params)


class Run(Command):
    def __init__(self, directory, control_mode, params, exchanges):
        self._params = params
        self._exchanges = exchanges
        self._directory = directory
        self._control_mode = control_mode

    def __call__(self, clock_factory):
        clock_factory(self._exchanges)
        self._control_mode.add_strategies(self._directory, self._params)
