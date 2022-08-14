from os import path

import toml

_config_path = path.expanduser('~/.pluto_config.toml')

configuration = toml.load(_config_path)
