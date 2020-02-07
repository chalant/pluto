import signal
import click

from pluto.interface import directory
from pluto.dev import dev

_SERVER = dev.Server()


def termination_handler(signum, frame):
    _SERVER.stop()


def interruption_handler(signum, frame):
    _SERVER.stop()


signal.signal(signal.SIGINT, interruption_handler)
signal.signal(signal.SIGTERM, termination_handler)


@click.group()
def cli():
    pass


@cli.command()
@click.option('--address', default='[::]:50051')
def start(address):
    with directory.Directory() as d:
        _SERVER.initialize(d, address)
        _SERVER.serve()

if __name__ == '__main__':
    cli()