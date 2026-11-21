import shutil

from os import path

import toml

from directory import directory as dr


def create_directory(root_path):
    root_dir = dr.create_directory(root_path, 'root')

    root_dir.add_file('setup', 'yaml')

    setup_dir = root_dir.add_directory('setup')

    setup_dir.add_file('run_setup', 'yaml')
    setup_dir.add_file('metadata', 'sqlite')

    data_dir = root_dir.add_directory('data')
    data_dir.add_file('metadata', 'sqlite')

    # universe_engine = create_engine('sqlite://' + universes_dir.get_file('metadata', 'sqlite').path)

    strategies_dir = root_dir.add_directory('strategies')
    strategy_states_dir = strategies_dir.add_directory('states')
    strategy_performance_dir = strategies_dir.add_directory('performance')

    root_dir.add_file('metadata', 'sqlite')

    return root_dir


def add_strategy_script(root_dir, name, source_path):
    """Copy the strategy script at source_path to <root>/strategies/<name>.py.

    ControllableService.Initialize reads the script of strategy <name> from
    that file (interim convention: no strategy database; the strategy store
    will be redesigned). Raises FileExistsError if the file exists.
    """
    strategies_dir = root_dir.get_directory('strategies')

    if path.exists(path.join(strategies_dir.path, name + '.py')):
        raise FileExistsError('Strategy of name {} already exists'.format(name))

    fl = strategies_dir.add_file(name, 'py')
    shutil.copyfile(source_path, fl.path)
    return fl




