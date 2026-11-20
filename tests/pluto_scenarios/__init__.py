"""Scenario strategies run in plain Zipline and in Pluto
(tests/test_pluto_scenarios.py). SCENARIO_MODULES has the shape of
pluto.examples.EXAMPLE_MODULES: name -> (_test_args(), path of the file);
MODULES: name -> module (for the Zipline run)."""
from importlib import import_module
import os

# the api functions are registered on pluto.api/zipline.api when the algorithm
# modules are imported; the scenario modules star-import them.
import zipline.algorithm  # noqa: F401
import pluto.algorithm  # noqa: F401

MODULES = {}
SCENARIO_MODULES = {}
for f in sorted(os.listdir(os.path.dirname(__file__))):
    if not f.endswith('.py') or f in ('__init__.py', 'common.py'):
        continue
    modname = f[:-len('.py')]
    mod = import_module('tests.pluto_scenarios.{}'.format(modname))
    MODULES[modname] = mod
    SCENARIO_MODULES[modname] = (mod._test_args(), mod.__spec__.origin)

    del f, modname, mod
