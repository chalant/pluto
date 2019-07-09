import click

@click.group()
def cli():
    pass

@cli.commmand()
@click.option('--address', default='[::]:50051')
@cli.option('--broker', default=None)
def start(address, broker):
    pass