import abc

from pluto.broker import broker_stub
from pluto.control.controllable import live_controllable
from pluto.control.controllable import simulation_controllable


class ControllableFactory(abc.ABC):
    def get_controllable(self, mode, session_id):
        controllable = self._create_controllable(mode, session_id)
        if not controllable:
            raise ValueError(
                '{} mode is not supported possible modes are: [simulation, live]'
            )
        return controllable

    @abc.abstractmethod
    def _create_controllable(self, mode, session_id):
        raise NotImplementedError


class ControllableProcessFactory(object):
    def __init__(self, channel):
        '''

        Parameters
        ----------
        channel: grpc._channel.Channel
        '''
        self._channel = channel

    def _create_controllable(self, mode, session_id):
        if mode == 'live':
            return live_controllable.LiveControllable(
                broker_stub.ProcessBrokerStub(
                    self._channel,
                    session_id))
        elif mode == 'simulation':
            return simulation_controllable.SimulationControllable()
