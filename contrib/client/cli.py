import pandas as pd

import datetime as dt

import sys, signal

import time

from contrib.coms.utils import certification as crt

import click

@click.command()
@click.argument('path', type=click.Path())
@click.argument('name', type=click.STRING)
def register_strategy(path, name):
    """Registers a strategy. Copies the python file and stores the file in some location"""
    pass

@click.command('run_simulation')
@click.argument('name', help='The registered strategy name', type=click.STRING)
@click.option('--capital', default=1000000, type=click.FLOAT)
@click.argument('start_session', help='Format : dd/mm/yy')
@click.option('--end_session', default = pd.Timestamp.utcnow().normalize(), help='Format : dd/mm/yy')
@click.option('--maximum_leverage', default=1.0)
def run_simulation(name, capital, start_session, end_session, maximum_leverage):
    start_session = pd.Timestamp(start_session).tz_localize(tz='UTC').normalize()
    if not isinstance(end_session, pd.Timestamp):
        end_session = pd.Timestamp(end_session).tz_localize(tz='UTC').normalize()
    """runs a strategy in simulation mode. Spawns a controllable sub-process and registers it to the server. The
    server then starts running the strategy"""
    pass

@click.command('run_live')
@click.argument('username')
@click.password_option()
def run_live(username, password):
    pass