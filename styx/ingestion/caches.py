import mmap
import itertools
import asyncio
import pickle

class Error(object):
	def __init__(self):
		self.task = ''
		self.error = ''

class MmapCachePool(object):
	def __init__(self, limit=100):
		self._free = []
		self._limit = limit
		self._lock = asyncio.Lock()
		self._available = asyncio.Event()

	async def get_cache(self):
		with self._lock:
			if not self._free and len(self._free) != self._limit:
				cache = MMapCache()
				self._free.append(cache)
				return cache
			self._available.wait()
			cache = self._free.pop()
			self._available.clear()
			return cache

	def put_cache(self, cache):
		with self._lock:
			self._free.append(cache)
			self._available.set()
	
	def __iter__(self):
		return itertools.chain(self._free)

	def __enter__(self):
		return self
	
	def __exit__(self):
		for cache in self._free:
			cache.close()

class MMapCache(object):
	def __init__(self):
		self._mmap = mmap.mmap(-1, 0)
		self._size = 0
	
	def append(self, item):
		bytes_ = pickle.dumps(item)
		lb = len(bytes_)
		self._mmap.write(lb.to_bytes(4, 'big') + bytes_)
		self._size += lb
	
	def close(self):
		self._mmap.close()
	
	def __iter__(self):
		index = 0
		mp = self._mmap
		pl = 0
		while (index < self._size):
			pl = int.from_bytes(mp[index:index + 4], 'big')
			index += 4
			yield pickle.loads(mp[index:mp[index + pl]], use_list=False)
			index += pl
	
	def __enter__(self):
		return self
	
	def __exit__(self):
		self._mmap.close()