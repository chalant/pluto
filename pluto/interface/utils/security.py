import os

import hashlib

_HASHING_ALGO = 'sha512'
_ITERATIONS = 500000

_BYTES_ENCODING = 'utf-8'
_SALT_BYTES = 64

def get_hash(salt, password):
    return hashlib.pbkdf2_hmac(
        _HASHING_ALGO,
        password.encode(_BYTES_ENCODING),
        salt,
        _ITERATIONS)

def create_hash_salt(password):
    salt = os.urandom(64),
    hash_ = get_hash(salt, password.decode(_BYTES_ENCODING))
    return hash_, salt