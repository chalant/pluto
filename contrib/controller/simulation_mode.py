from . import controller
from concurrent import futures

import socket

#TODO: the controller should always send a "session-id" as metadata
# the client and the controller will exchange this session-id
#TODO: raise a grpc error if a request doesn't have a session-id...
#TODO: We should encapsulate the controller, since the behavior changes with the environment..
class SimulationControlMode(controller.ControlMode):
    '''server that controls'''

    class ControllerThread(object):
        def __init__(self):
            self._sessions = []
            self._pool = futures.ThreadPoolExecutor()

        def ClockUpdate(self, clock_evt):
            pass

        def add_session(self, session):
            self._sessions.append(session)

        def start(self):
            pool = self._pool
            #todo: a clock must be callable
            for clock in self._clocks:
                pool.submit(clock)

    #TODO: HOW TO CREATE A CLOCK? => scan through the list of country codes and assets
    # a clock takes a calendar as argument => how do we instanciate the "right" calendar?
    # maybe we should specify the exchange instead of the country?
    def __init__(self, address, local_hub, session_factory):
        super(SimulationControlMode, self).__init__(address, local_hub, session_factory)
        self._clocks = {}
        self._exchanges = exchanges = {}
        self._hub = local_hub

        for exc in local_hub.get_exchanges():
            country_code = exc.country_code
            self._append_to_dict(country_code, exc.name, exchanges)
            for at in exc.asset_types:
                self._append_to_dict(at, exc.name, exchanges)

    def _append_to_dict(self, key, value, dict_):
        v = dict_.get(key, None)
        if not v:
            dict_[key] = [value]
        else:
            dict_[key].append(value)

    def name(self):
        return 'simulation'

    def _create_address(self):
        #creates an address for the controllable
        #in simulation, creates a local controllable
        s = socket()
        s.bind(('', 0))
        address = 'localhost:{}'.format(s.getsockname()[1])
        s.close()
        return address

    def _run(self, sessions):
        '''

        Parameters
        ----------
        sessions

        Returns
        -------

        1)create clocks using the domain_struct of the session.
        2)

        '''

        clocks = self._clocks
        sess_per_exc = {}
        exc_set = set()
        for session in sessions:
            results = self._resolve_exchanges(session.domain_struct, self._exchanges)
            exc_set = exc_set & results
            for exc in results:
                self._append_to_dict(exc, session, sess_per_exc)
        #todo: create clocks
        #each clock


    def _resolve_exchanges(self, domain_struct, exchanges):
        cc = domain_struct.country_code
        at = domain_struct.asset_types

        # dictionary mapping keys like asset types and country codes, to sets of exchanges

        at_set = set()
        cc_set = set()

        # union of all exchanges trading the given asset types
        for c in at:
            at_set = at_set | exchanges[c]

        # union of all exchanges operating in the given countries
        cc_set = cc_set | exchanges[cc]

        # intersection of exchanges trading in the given countries and asset types
        return cc_set & at_set


    def _clock_update(self, clock_evt):

        pass