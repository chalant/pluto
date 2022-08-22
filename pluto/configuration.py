from os import path

import toml

configuration = toml.load(path.expanduser('~/.pluto_config.toml'))
