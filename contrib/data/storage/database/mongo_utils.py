from pymongo import MongoClient
import sys

_CLIENT = MongoClient(serverSelectionTimeoutMS=1)
_DATABASE = _CLIENT['Main']

def get_collection(name):
	return _DATABASE[name]




