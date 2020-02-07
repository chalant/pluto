from concurrent import futures

import grpc

import pandas as pd

from IPython.display import display

from pluto.dev import dev
from pluto.interface import directory
from pluto.coms.utils import conversions
from pluto.control.modes.processes import in_memory

from protos import controller_pb2
from protos import development_pb2
from protos import interface_pb2

framework_url = '[::]:50051'


class Client(object):
    def __init__(self, environment):
        self._env = environment

        self._start = None
        self._end = None
        self._session_id = None

        self._edt = environment._editor
        self._ctl = environment._controller
        self._exp = environment._explorer

    def setup(self,
              strategy_id,
              start,
              end,
              capital,
              max_leverage,
              universe,
              data_frequency,
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
                universe=universe,
                data_frequency=data_frequency,
                look_back=look_back), ())

        self._session_id = sess_id = response.session_id
        return sess_id

    def run(self, capital_ratio, max_leverage):
        session_id = self._session_id
        if not session_id:
            raise RuntimeError('No session was setup')

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
        return self._edt.New(
            development_pb2.NewStrategyRequest(
                name=name), ())


dir_ = directory.Directory()
d = dir_.__enter__()
env = dev.Environment(
    grpc.server(futures.ThreadPoolExecutor(10)),
    d,
    framework_url,
    in_memory.InMemoryProcessFactory())
client = Client(env)


def setup(strategy_id, capital=50000, max_leverage=1.0, universe='test'):
    start_ = pd.Timestamp('1990-01-02 00:00:00')
    display(
        client.setup(
            strategy_id,
            start_,
            start_ + pd.Timedelta(days=2000),
            capital, max_leverage,
            universe,
            'daily',
            150))


def run(capital_ratio=1.0, max_leverage=1.0):
    start_ = pd.Timestamp('1990-01-02 00:00:00')
    client.run(
        capital_ratio,
        max_leverage
    )


def add_strategy(name):
    b = b''
    for chunk in client.add_strategy(name):
        b += chunk.data
    display(b)


def get_strategy(strategy_id):
    print(client.get_strategy(strategy_id).decode('utf-8'))


def strategy_list():
    display([d for d in client.strategy_list()])