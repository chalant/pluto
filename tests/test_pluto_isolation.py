"""The in-memory simulation path must not import the third-party packages that
only removed code needed (they may still be installed in an older environment).
A child interpreter blocks them and runs the example backtests; it fails if any
of them gets imported.

The blocker is a ``find_spec`` finder on ``sys.meta_path`` (Python 3.12 removed
the ``find_module``/``load_module`` protocol, a finder written that way does not
block anything any more). A negative control proves that this finder blocks."""
import os
import subprocess
import sys
import unittest

_BLOCKER = r'''
import importlib.abc
import sys

BLOCKED = ('clickhouse_driver', 'PyQt6', 'PySide6')


class Blocker(importlib.abc.MetaPathFinder):
    def __init__(self, blocked):
        self._blocked = blocked

    def find_spec(self, name, path=None, target=None):
        if name.split('.')[0] in self._blocked:
            raise ImportError('blocked on the in-memory path: ' + name)
        return None


sys.meta_path.insert(0, Blocker(BLOCKED))
'''

_CHILD = _BLOCKER + r'''
import unittest

import tests.test_pluto_examples as t

suite = unittest.TestLoader().loadTestsFromTestCase(t.PlutoExamplesTests)
result = unittest.TextTestRunner(verbosity=2).run(suite)
loaded = sorted(m for m in sys.modules if m.split('.')[0] in BLOCKED)
print('blocked packages loaded:', loaded)
sys.exit(0 if result.wasSuccessful() and not loaded else 1)
'''

# negative control: the same finder, told to block a standard library module
# that is certainly importable, must make its import fail with the blocker's
# message (and the module must be absent from sys.modules afterwards)
_CONTROL = _BLOCKER + r'''
sys.meta_path[0] = Blocker(('colorsys',))
try:
    import colorsys
except ImportError as e:
    assert 'blocked on the in-memory path: colorsys' in str(e), e
    assert 'colorsys' not in sys.modules
    print('control: blocked')
else:
    print('control: NOT blocked')
    sys.exit(1)
'''

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _run_child(code, timeout):
    env = dict(os.environ, PYTHONPATH=_ROOT)
    return subprocess.run(
        [sys.executable, '-c', code],
        cwd=_ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True,
        timeout=timeout)


class InMemoryPathIsolationTest(unittest.TestCase):
    def test_blocker_blocks(self):
        proc = _run_child(_CONTROL, 120)
        self.assertEqual(proc.returncode, 0, proc.stdout[-4000:])
        self.assertIn('control: blocked', proc.stdout)

    def test_examples_run_without_removed_packages(self):
        proc = _run_child(_CHILD, 900)
        self.assertEqual(proc.returncode, 0, proc.stdout[-4000:])
        self.assertIn('blocked packages loaded: []', proc.stdout)
