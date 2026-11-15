"""The in-memory simulation path must not depend on the packages that will be
superseded (gui/, styx/) nor on the GUI toolkit. A child interpreter blocks them
and runs the example backtests; it fails if any of them gets imported."""
import os
import subprocess
import sys
import unittest

_CHILD = r'''
import sys
import unittest

BLOCKED = ('gui', 'styx', 'PyQt6', 'PySide6')


class Blocker(object):
    def find_module(self, name, path=None):
        if name.split('.')[0] in BLOCKED:
            return self

    def load_module(self, name):
        raise ImportError('blocked on the in-memory path: ' + name)


sys.meta_path.insert(0, Blocker())

import tests.test_pluto_examples as t

suite = unittest.TestLoader().loadTestsFromTestCase(t.PlutoExamplesTests)
result = unittest.TextTestRunner(verbosity=2).run(suite)
loaded = sorted(m for m in sys.modules if m.split('.')[0] in BLOCKED)
print('blocked packages loaded:', loaded)
sys.exit(0 if result.wasSuccessful() and not loaded else 1)
'''

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class InMemoryPathIsolationTest(unittest.TestCase):
    def test_examples_run_without_gui_and_styx(self):
        env = dict(os.environ, PYTHONPATH=_ROOT)
        proc = subprocess.run(
            [sys.executable, '-c', _CHILD],
            cwd=_ROOT,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
            timeout=900)
        self.assertEqual(proc.returncode, 0, proc.stdout[-4000:])
        self.assertIn('blocked packages loaded: []', proc.stdout)
