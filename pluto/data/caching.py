from abc import ABC, abstractmethod


class Cache(ABC):
    @abstractmethod
    def get(self, field):
        raise NotImplementedError

    @abstractmethod
    def set(self, field, data):
        raise NotImplementedError


class InMemoryCache(Cache):
    def __init__(self):
        self._cache = {}

    def get(self, field):
        return self._cache[field]

    def set(self, field, data):
        self._cache[field] = data
