import click
import grpc
import pandas as pd

from pluto.coms.utils import conversions

from protos import development_pb2_grpc as dev
from protos import interface_pb2_grpc as interface
from protos import development_pb2
from protos import interface_pb2
from protos import controller_pb2_grpc as controller
from protos import controller_pb2


class Client(object):
    def __init__(self, url):
        self._environment = dev
        channel = grpc.insecure_channel(url)
        self._env = dev.EnvironmentStub(channel)
        self._edt = dev.EditorStub(channel)
        self._exp = interface.ExplorerStub(channel)
        self._ctl = controller.ControllerStub(channel)

        self._session_id = None
        self._end = None

    def setup(self,
              strategy_id,
              start,
              end,
              capital,
              max_leverage,
              universe,
              data_frequency,
              look_back):
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
                look_back=look_back))
        self._session_id = sess_id = response.session_id
        return sess_id

    def run(self, session_id, capital_ratio, max_leverage, end):
        self._ctl.Run(
            controller_pb2.RunRequest(
                run_params=[
                    controller_pb2.RunParams(
                        session_id=session_id,
                        capital_ratio=capital_ratio,
                        max_leverage=max_leverage)],
                end=conversions.to_proto_timestamp(end)))

    def strategy_list(self):
        return self._exp.StrategyList(
            interface_pb2.StrategyFilter())

    def get_strategy(self, strategy_id):
        b = b''
        for chunk in self._edt.GetStrategy(
                development_pb2.StrategyRequest(
                    strategy_id=strategy_id)):
            b += chunk.data
        return b

    def add_strategy(self, name):
        return self._edt.New(
            development_pb2.NewStrategyRequest(
                name=name
            )
        )


_CLIENT = Client('[::]:50051')


@click.group()
def cli():
    pass


@cli.command()
@click.argument('strategy_id')
@click.option('--capital', default=50000)
@click.option('--max-leverage', default=1.0)
@click.option('--universe', default='test')
def setup(strategy_id, capital, max_leverage, universe):
    start_ = pd.Timestamp('1990-01-02 00:00:00')
    click.echo(_CLIENT.setup(
        strategy_id,
        start_,
        start_ + pd.Timedelta(days=2000),
        capital,
        max_leverage,
        universe,
        'daily',
        150))


@cli.command()
def strategy_list():
    for strategy_id in _CLIENT.strategy_list():
        click.echo(strategy_id)


@cli.command()
@click.argument('strategy_id')
def get_strategy(strategy_id):
    click.echo(_CLIENT.get_strategy(strategy_id))


@cli.command()
@click.argument('name')
def add_strategy(name):
    b = b''
    for chunk in _CLIENT.add_strategy(name):
        b += chunk.data
    click.echo(b)


@cli.command()
@click.argument('session_id')
@click.option('--capital-ratio', default=1.0)
@click.option('--max-leverage', default=1.0)
def run(session_id, capital_ratio, max_leverage):
    start_ = pd.Timestamp('1990-01-02 00:00:00')
    click.echo(_CLIENT.run(
        session_id,
        capital_ratio,
        max_leverage,
        start_ + pd.Timedelta(days=2000)))


if __name__ == '__main__':
    cli()
