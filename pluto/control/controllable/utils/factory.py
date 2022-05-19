import abc

from pluto.broker import broker_stub

from pluto.control.controllable import live_controllable
from pluto.control.controllable import simulation_controllable


class ControllableParams(object):
    __slots__ = ['mode', 'session_id']

    def __init__(self, mode, session_id):
        self.mode = mode
        self.session_id = session_id


class ControllableFactory(abc.ABC):
    def get_controllable(self, params):
        """

        Parameters
        ----------
        params: ControllableParams

        Returns
        -------

        """
        controllable = self._create_controllable(params)
        if not controllable:
            raise ValueError(
                '{} mode is not supported possible modes are: [simulation, live]'
            )
        return controllable

    @abc.abstractmethod
    def _create_controllable(self, params):
        raise NotImplementedError


class RemoteControllableFactory(ControllableFactory):
    def __init__(self, channel):
        '''

        Parameters
        ----------
        channel: grpc._channel.Channel
        '''
        self._channel = channel

    def _create_controllable(self, params):
        if params.mode == 'live':
            return live_controllable.LiveControllable(
                broker_stub.ProcessBrokerStub(
                    self._channel,
                    params.session_id))
        elif params.mode == 'backtesting':
            return simulation_controllable.SimulationControllable()
