from concurrent import futures

import grpc

from pluto.coms.utils import conversions
from pluto.dev import dev

from protos import controller_pb2
from protos import development_pb2
from protos import interface_pb2


framework_url = '[::]:50051'

class TestClient(object):
    def __init__(self, process_factory, directory):
        self._env = env = dev.Environment(
            grpc.server(futures.ThreadPoolExecutor(10)),
            directory,
            framework_url,
            process_factory)
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
                look_back=look_back), ())

        self._session_id = sess_id = response.session_id
        return sess_id

    def run(self, session_id, capital_ratio, max_leverage):
        self._env._controller.Run(
            controller_pb2.RunRequest(
                run_params=[
                    controller_pb2.RunParams(
                        session_id=session_id,
                        capital_ratio=capital_ratio,
                        max_leverage=max_leverage)],
                end=conversions.to_proto_timestamp(self._end)), ())

    def strategy_list(self):
        return self._exp.StrategyList(
            interface_pb2.StrategyFilter(), ())

    def get_strategy(self, strategy_id):
        b = b''
        for chunk in self._edt.GetStrategy(
                development_pb2.StrategyRequest(
                    strategy_id=strategy_id), ()):
            b += chunk.data
        return b

    def add_strategy(self, name):
        b = b''
        for chunk in self._edt.New(
                development_pb2.NewStrategyRequest(name=name),
                ()):
            b += chunk.data
        response = development_pb2.NewStrategyResponse()
        response.ParseFromString(b)
        return response

    def save(self, strategy_id, implementation):
        self._edt.Save()

    def watch(self, session_id):
        pass