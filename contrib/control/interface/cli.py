import signal
import threading

import click

from contrib.control.interface import interface

stop_event = threading.Event()

def handle_termination(signum, frame):
    stop_event.set()

def handle_interruption(signum, frame):
    stop_event.set()

signal.signal(signal.SIGTERM, handle_termination)
signal.signal(signal.SIGINT, handle_interruption)

#todo: add some initial setup prompt (if there are no stored credentials) the provided
# credentials will be used to authenticate the user when connecting to the service.
# the user that launches the server for the first time is the admin: he can then decide
# to add users or not.

@click.group()
def cli():
    pass

@cli.command()
@click.option('--port', default='50051')
def start(port):
    account = interface.Account()
    account.start(port)
    stop_event.wait()
    account.stop()

if __name__ == '__main__':
    cli()