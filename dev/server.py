import click

from pluto.interface import directory
from pluto.server import server
from pluto.dev import dev


@click.group()
def cli():
    pass


@cli.command()
@click.option('--address', default='[::]:50051')
@click.option('--test', is_flag=True)
@click.option('-re', '--recovery', is_flag=True)
def start(address, test):
    env = 'test' if test else 'pluto'
    #todo: recovery
    with directory.get_directory(env) as d:
        srv = server.get_server(
            dev.DevService(
                d,
                address,

            ))
        srv.serve(address)


if __name__ == '__main__':
    cli()
