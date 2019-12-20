from abc import ABC, abstractmethod

class Command(ABC):
    @abstractmethod
    def __call__(self, control_mode, clock_factory):
        raise NotImplementedError

class Stop(Command):
    def __init__(self, params, liquidate=False):
        self._liquidate = liquidate
        self._params = params


    def __call__(self, control_mode, clock_factory):
        mode = control_mode
        sessions = mode.sessions
        liquidate = self._liquidate

        to_stop = set(session.id for session in sessions) & set(p.session_id for p in self._params)

        if liquidate:
            for session in to_stop:
                mode._liquidate(session)
        else:
            for session in to_stop:
                mode.stop(session)

class Setup(Command):
    def __init__(self, params, exchanges):
        self._params = params
        self._exchanges = exchanges

    def __call__(self, control_mode, clock_factory):
        '''
        runs a session
            1)load/create and store domain
            2)load/create and store sessions (and strategies)
        '''

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

        mode = control_mode
        #the mode manages capital assignments and leverage ratios..

        #todo: exchanges are computed from universes externally.
        exchanges = self._exchanges
        clock_factory(exchanges)
        mode.add_strategies(self._params)
