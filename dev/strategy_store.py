from os import path

from sqlalchemy import create_engine

from dev import events

from pluto.strategies import strategies
from pluto.strategies import schema


class StrategyLoader(object):
    def __init__(self, meta, root_dir):
        self._meta = meta
        self._root_dir = root_dir

    def load_strategy(self, mode="r"):
        with open(path.join(
                self._root_dir.path,
                self._meta.strategy_name + ".py"),
                mode) as f:
            return f.read()


# todo: add the ability to cache, since multiple objects share this
class StrategyStore(object):
    def __init__(self, directory):
        self._strategy_dir = dir_ = directory.get_directory('strategies')
        self._strategy_db_engine = engine = create_engine(
            'sqlite:///' + dir_.get_file('metadata', 'sqlite').path)

        schema.meta.create_all(engine)

        self._strategies = []
        self._ids = {}

    def add_strategy(self, name, template_path):
        with self._strategy_db_engine.connect() as connection:
            meta = strategies.create_strategy(
                connection,
                strategies.StrategyCreator(
                    self._strategy_dir,
                    template_path),
                name)

            self._strategies.append(meta)

            return meta

    def load_strategy(self, strategy_meta):
        return StrategyLoader(
            strategy_meta,
            self._strategy_dir)

    def get_strategies(self):
        # load a cache strategies
        if not self._strategies:
            stg = self._strategies

            with self._strategy_db_engine.connect() as connection:
                i = 0
                for s in strategies.get_strategies(connection):
                    stg.append(s)
                    i += 1

        self._ids = set(self._strategies)

        return self._strategies

    def get_strategy_at_index(self, index):
        return self._strategies[index]

    def get_strategy_of_name(self, name):
        with self._strategy_db_engine.connect() as connection:
            try:
                strategies.get_strategy(connection, name)
            except FileNotFoundError:
                return False

    def contains_strategy_of_name(self, name):
        for stg in self._strategies:
            if stg.name == name:
                return True
        return False

    def contains_strategy(self, meta):
        return hash(meta) in self._ids

    def __contains__(self, item):
        if type(item) == str:
            return self.contains_strategy_of_name(item)
        elif type(item) == strategies.Strategy:
            return self.contains_strategy(item)

        return False

    def __getitem__(self, item):
        if type(item) == int:
            return self.get_strategy_at_index(item)
        elif type(item) == str:
            return self.get_strategy_of_name(item)
        return

