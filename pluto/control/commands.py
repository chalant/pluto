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
    __slots__ = ['_control_mode', '_params', '_liquidate']

    def __init__(self, control_mode, params, liquidate=False):
        self._liquidate = liquidate
        self._params = params
        self._control_mode = control_mode

    def __call__(self, clock_factory):
        self._control_mode.stop(self._params)


class Run(Command):
    __slots__ = [
        '_directory',
        '_control_mode',
        '_params',
        '_start',
        '_end']

    def __init__(self,
                 directory,
                 control_mode,
                 params,
                 start,
                 end):
        self._params = params
        self._directory = directory
        self._control_mode = control_mode

        self._start = start
        self._end = end

    def __call__(self, clock_factory):
        exchanges = \
            self._control_mode.add_strategies(
                self._directory,
                self._params)
        clock_factory(exchanges)
