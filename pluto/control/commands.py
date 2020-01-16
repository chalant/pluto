from abc import ABC, abstractmethod


class Command(ABC):
    @abstractmethod
    def _command(self):
        raise NotImplementedError(self._command.__name__())

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

    @abstractmethod
    def __add__(self, other):
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

        # todo: we need to make sure that all the positions have been liquidated before adding new
        # sessions and updating parameters, HOW?
        # possible solution: update capital each end of session...
        # problem? we are rebalancing capital, which means that all returns are redestributed
        # accross all strategies...
        # capital is redistributed each session_end event? => should let the client decide...
        # we still need a solution for distributing cash from liquidated assets...
        # maybe put the cash in a special variable (un-assigned capital) in the tracker object
        # the tracker could update the sessions capital as it receives new un-assigned capital...
        # each capital change event is documented in the performance packet.

        # schedule all sessions that are not in the params for liquidation

        #the mode manages capital assignments and leverage ratios..
        clock_factory(self._exchanges)
        control_mode.add_strategies(self._directory, self._params)

