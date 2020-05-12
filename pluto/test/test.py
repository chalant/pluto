import grpc

from pluto.coms.utils import conversions
from pluto.dev import dev
from pluto.utils import stream
from pluto.control.modes.processes import in_memory

from protos import controller_pb2
from protos import development_pb2
from protos import interface_pb2


class InMemoryTestClient(object):
    def __init__(self,
                 directory,
                 framework_url,
                 mode_factory,
                 loop_factory,
                 thread_pool):
        '''

        Parameters
        ----------
        directory
        framework_url
        mode_factory: pluto.control.modes.utils.ModeFactory
        loop_factory
        '''

        server = grpc.server(thread_pool)
        self._mode_type = mode_factory.mode_type
        self._env = env = dev.DevService(
            server,
            directory,
            framework_url,
            mode_factory,
            loop_factory,
            in_memory.InMemoryProcessFactory(
                directory))

        self._start = None
        self._end = None
        self._session_id = None

        self._edt = env._editor
        self._ctl = env._controller
        self._exp = env._explorer

    def setup(self,
              strategy_id,
              start,
              end,
              capital,
              max_leverage,
              look_back):

        self._start = start
        self._end = end
        response = self._env.Setup(
            development_pb2.SetupRequest(
                strategy_id=strategy_id,
                capital=capital,
                max_leverage=max_leverage,
                start=conversions.to_proto_timestamp(start),
                end=conversions.to_proto_timestamp(end),
                universe='test',
                data_frequency='daily',
                look_back=look_back),
            in_memory.FakeContext(()))

        self._session_id = sess_id = response.session_id
        return sess_id

    def run(self, session_id, capital_ratio, max_leverage):
        self._env._controller.Run(
            controller_pb2.RunRequest(
                mode=self._mode_type,
                run_params=[
                    controller_pb2.RunParams(
                        session_id=session_id,
                        capital_ratio=capital_ratio,
                        max_leverage=max_leverage)],
                end=conversions.to_proto_timestamp(self._end)),
            in_memory.FakeContext(()))

    def strategy_list(self):
        return self._exp.StrategyList(
            interface_pb2.StrategyFilter(),
            in_memory.FakeContext(()))

    def get_strategy(self, strategy_id):
        b = b''
        for chunk in self._edt.GetStrategy(
                development_pb2.StrategyRequest(
                    strategy_id=strategy_id),
                in_memory.FakeContext(())):
            b += chunk.data
        return b

    def add_strategy(self, name):
        b = b''
        for chunk in self._edt.New(
                development_pb2.NewStrategyRequest(
                    name=name),
                in_memory.FakeContext(())):
            b += chunk.data
        response = development_pb2.NewStrategyResponse()
        response.ParseFromString(b)
        return response

    def save(self, strategy_id, implementation):
        request = development_pb2.SaveRequest(
            strategy_id=strategy_id,
            strategy=implementation
        ).SerializeToString()
        self._edt.Save(
            stream.chunk_bytes(request),
            in_memory.FakeContext(()))

    def watch(self, session_id):
        for perf in self._env._monitor.Watch(
                interface_pb2.WatchRequest(
                    session_id=session_id),
                in_memory.FakeContext(())):
            packet = controller_pb2.PerformancePacket()
            packet.ParseFromString(perf.packet)
            yield conversions.from_proto_performance_packet(packet)

    def stop_watching(self, session_id):
        self._env._monitor.StopWatching(
            interface_pb2.StopWatchingRequest(
                session_id=session_id),
            in_memory.FakeContext(()))
