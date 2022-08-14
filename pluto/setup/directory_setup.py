import toml

from sqlalchemy import create_engine

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
    strategy_metadata = strategies_dir.add_file('metadata', 'sqlite')

    root_dir.add_file('metadata', 'sqlite')

    strategies_db_engine = create_engine(
        'sqlite://' + strategy_metadata.path)

    return root_dir




