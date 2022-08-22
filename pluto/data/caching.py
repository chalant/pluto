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

    def __contains__(self, item):
        return item in self._cache


class RedisCache(Cache):
    def __init__(self, host, port):
        raise NotImplementedError("Redis not supported yet!")

    def get(self, field):
        raise NotImplementedError

    def set(self, field, data):
        raise NotImplementedError

    def __contains__(self, item):
        raise NotImplementedError
