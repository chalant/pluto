from os import path

from sqlalchemy import create_engine

from pluto.strategies import strategies as stg


class StrategyLoader(object):
    def __init__(self, meta, strategy_dir):
        self._root_dir = strategy_dir
        self._meta = meta

    @property
    def strategy_name(self):
        return self._meta.strategy_name

    def load(self):
        with open(path.join(
                self._root_dir.path,
                self._meta.strategy_name + ".py")) as f:
            return f.read()


class StrategySetup(object):
    __slots__ = ['capital']

    def __init__(self):
        self.capital = 0


class StrategyStore(object):
    def __init__(self, directory):
        self._directory = directory

        self._strategy_dir = dir_ = directory.get_directory('strategies')
        self._engine = create_engine(
            'sqlite:///' + dir_.get_file('metadata', 'sqlite').path)

        self._strategies = []

    @property
    def strategies(self):
        return self._strategies

    def add_strategy(self, name):
        with self._engine.connect() as connection:
            meta = stg.get_strategy(
                connection,
                name)

            st = StrategySetup()

            self._strategies.append((
                st,
                StrategyLoader(
                    meta,
                    self._strategy_dir)))

            return st


class Setup(object):
    def __init__(
            self,
            strategy_store,
            account):
        self._strategy_store = strategy_store
        self._account = account

    @property
    def account(self):
        return self._account

    def add_strategy(self, name):
        return self._strategy_store.add_strategy(name)
