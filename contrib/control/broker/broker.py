from abc import abstractmethod, ABC

class Broker(ABC):
    def __init__(self):
        self._sessions = {}

        self._to_add = []
        self._to_update = []
        self._to_liquidate = []

    @property
    def sessions(self):
        return self._sessions.values()

    def add_sessions(self, sessions):
        sess = self._sessions
        for session in sessions:
            sess[session.id] = session

    def update_sessions(self, pairs):
        sess = self._sessions
        for session, params in pairs:
            sess[session.id].update_parameters(params)

    def liquidate(self, sessions):
        sess = self._sessions
        for session in sessions:
            self._liquidate_one(sess.pop(session.id))

    def update(self, dt):
        # todo: process all the requests : adds new sessions, updates previous sessions and liquidates
        # sessions

        #todo: we need to track the sessions that are liquidated so that we can re-assign their
        # capital to alive sessions. => in simulation mode, there is no need to track
        # liquidated sessions... => delegate to the tracker

        #todo: how do we know if a session has no more positions?
        # the trader (controllable) has a positions property and if it has no positions, we can
        # remove from to_liquidate dict...

        #todo: cash from transactions made by session in liquidation is stored in some special
        # variable, so it can be assigned to running sessions.

        # todo: send broker data to the controllables
        #  in simulation, mode, this does nothing, since the data is already controllable-side
        self._update_account(dt, self._sessions.values())

    def add_session(self, session):
        self._to_add.append(session)

    def update_session(self, session, params):
        self._to_update.append((session, params))

    def get_session(self, sess_id):
        return self._sessions.get(sess_id, None)

    @abstractmethod
    def _liquidate_one(self, session):
        raise NotImplementedError

    @abstractmethod
    def _update_account(self, dt, sessions):
        raise NotImplementedError