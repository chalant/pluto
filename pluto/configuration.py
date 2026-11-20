from os import path, environ

import toml

# The configuration file is optional: the in-memory simulation path does not
# read any key from it. Code that needs settings (alpaca, polygon.io...)
# raises a KeyError when its section is missing.
# PLUTO_CONFIG points to another file (which must then exist).
_DEFAULT_PATH = path.expanduser('~/.pluto_config.toml')

if 'PLUTO_CONFIG' in environ:
    configuration = toml.load(environ['PLUTO_CONFIG'])
elif path.isfile(_DEFAULT_PATH):
    configuration = toml.load(_DEFAULT_PATH)
else:
    configuration = {}
