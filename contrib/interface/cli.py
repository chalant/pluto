import os

import signal

import hashlib

import threading

import click

from contrib.interface import interface, credentials
from contrib.interface.utils import security

stop_event = threading.Event()

def handle_termination(signum, frame):
    stop_event.set()

def handle_interruption(signum, frame):
    stop_event.set()

signal.signal(signal.SIGTERM, handle_termination)
signal.signal(signal.SIGINT, handle_interruption)

def _setup_password():
    password1 = click.prompt('Enter Password', type=str, hide_input=True)
    password2 = click.prompt('Confirm Password', type=str, hide_input=True)
    if password1 == password2:
        click.echo('Confirmation Successful')
        if click.confirm('Proceed with the password?'):
            return password1
        else:
            return _setup_password()
    else:
        click.echo('Confirmation Failed')
        return _setup_password()

def _reset_credentials(session):
    if click.confirm('Proceed?'):
        value = session.query(credentials.Credentials).filter_by(is_local=True).first()
        if value:
            session.delete(value)
        return True
    else:
        return False

def _get_credentials(session):

    return session.query(credentials.Credentials)\
        .filter(credentials.Credentials, 'is_local' == True).first()


def _setup_credentials(session):
    username = click.prompt('Enter Username', type=str)
    password = _setup_password()
    if click.confirm('Save credentials?'):
        hash_, salt = security.create_hash_salt(password)
        creds = credentials.Credentials(
            username=username,
            salt=salt,
            hash_=hash_,
            is_admin=True,
            is_local=True)
        click.echo('Saving credentials')
        session.add(creds)
        click.echo('Done')
        return creds
    else:
        click.prompt('')
        if click.confirm('Start from begining?', abort=True):
            return _setup_credentials(session)

@click.group()
def cli():
    pass

@cli.command()
@click.option('--port', default='50051')
@click.option('-rc','--reset-crendentials', is_flag=True)
def start(port, reset_credentials):
    #set up the initial credentials
    with credentials.write() as s:
        creds = _get_credentials(s)
        if creds:
            #reset only if credentials are found and the flag is provided
            if reset_credentials:
                reset = _reset_credentials(s)
                if reset:
                    creds = _setup_credentials(s)
        else:
            creds = _setup_credentials(s)

    click.echo('Launching server...')
    #todo: we need additional setup for the brokers etc. must be done on initial setup
    # since these are critical. => note: the brokers lifecycle must be controlled by this server.
    # maybe expose some "interface" for communicating with broker.
    # also provide credentials for each broker (paper, live, name)
    # note: by default, we have a simulation broker.
    account = interface.Account(creds)
    account.start(port)
    click.echo('Server listening on: {}'.format('localhost:{}'.format(port)))
    stop_event.wait()
    account.stop()

if __name__ == '__main__':
    cli()