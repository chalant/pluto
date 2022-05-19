from os import path

from sqlalchemy import create_engine

from directory import directory as dr

root_dir = dr.create_directory(path.expanduser('~./pluto'), 'root')
root_dir.add_file('setup', 'json')

setup_dir = root_dir.add_directory('setup')
setup_dir.add_file('run_setup', 'json')
setup_dir.add_file('metadata', 'sqlite')

universes_dir = root_dir.add_directory('universes')
universes_dir.add_file('metadata', 'sqlite')

# universe_engine = create_engine('sqlite://' + universes_dir.get_file('metadata', 'sqlite').path)

strategies_dir = root_dir.add_directory('strategies')
strategy_states_dir = strategies_dir.add_directory('states')
strategy_performance_dir = strategies_dir.add_directory('performance')

root_dir.add_file('metadata', 'sqlite')

strategies_db_engine = create_engine(
    'sqlite://' + strategies_dir.get_file('metadata', 'sqlite').path)


def get_directory(name):
    """

    Parameters
    ----------
    name

    Returns
    -------
    dr._Directory
    """
    res = root_dir.get_directory(name)

    if res:
        return res
    else:
        raise FileNotFoundError('No directory named ', name)


def get_file_in_directory(file_name, extension, dir_):
    """

    Parameters
    ----------
    file_name
    extension
    dir_

    Returns
    -------
    dr._File

    """
    res = dir_.get_file(file_name, extension)

    if res:
        return res.pop()

    raise FileNotFoundError('Directory {} does not contain file {} of type {}'.format(dir_name, file_name, extension))


