from sqlalchemy import create_engine

from directory import directory as dr
from directory import loader

from pluto.strategies import templates


def create_directory(root_path):
    root_dir = dr.create_directory(root_path, 'root')

    root_dir.add_file('setup', 'yaml')

    setup_dir = root_dir.add_directory('setup')

    setup_dir.add_file('run_setup', 'yaml').touch()
    setup_dir.add_file('metadata', 'sqlite').touch()

    data_dir = root_dir.add_directory('data')
    data_dir.add_file('metadata', 'sqlite').touch()

    # universe_engine = create_engine('sqlite://' + universes_dir.get_file('metadata', 'sqlite').path)

    strategies_dir = root_dir.add_directory('strategies')
    strategy_states_dir = strategies_dir.add_directory('states')
    strategy_performance_dir = strategies_dir.add_directory('performance')
    strategy_metadata = strategies_dir.add_file('metadata', 'sqlite')
    strategy_metadata.touch()

    templates_dir = root_dir.add_directory('templates')
    launchers_dir = root_dir.add_directory('launchers')

    with open(templates_dir.add_file('pipeline', 'py').path, 'a') as f:
        f.write(templates.pipeline_template)

    root_dir.add_file('metadata', 'sqlite').touch()

    strategies_db_engine = create_engine(
        'sqlite://' + strategy_metadata.path)

    loader.load_directory(root_dir)

    return root_dir