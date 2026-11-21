"""Adds the expected results of the examples for the pandas version in use to
tests/resources/example_data.tar.gz (PM19: the pandas 0.22 results stay as they
are, as a reference).

The results are what zipline's own run path produces on the vendored zipline
(zipline.examples.run_example -> run_algorithm) on the example data of the
tarball, with the very benchmark returns Pluto reads. Pluto must then equal them
bit for bit (tests/test_pluto_examples.py).

Usage (from the root of the repository, in the environment of the tests)::

    python tests/resources/add_expected_perf.py

The set is stored under ``example_data/expected_perf/<pandas version with dashes>``
(``2-3-3`` for pandas 2.3.3); an existing set of that name is replaced.
"""
import shutil
import tarfile

import pandas as pd

from zipline.data.bundles import register
from zipline.testing import test_resource_path, tmp_dir
from zipline.utils.cache import dataframe_cache
from zipline import examples

from tests.test_pluto_zipline_oracle import benchmark_returns, zipline_example_modules


def main():
    example_path = test_resource_path('example_data.tar.gz')
    key = pd.__version__.replace('.', '-')

    with tmp_dir() as d:
        with tarfile.open(example_path) as tar:
            tar.extractall(d.path, filter='data')

        # the data are in the tarball (ZIPLINE_ROOT), nothing to ingest
        environ = {'ZIPLINE_ROOT': d.getpath('example_data/root')}
        register('test', lambda *args, **kwargs: None)

        path = d.getpath('example_data/expected_perf/%s' % key)
        shutil.rmtree(path, ignore_errors=True)

        returns = benchmark_returns(environ)

        # not as a context manager: leaving it removes the directory
        cache = dataframe_cache(path, serialization='pickle:2')
        for name, module in sorted(zipline_example_modules().items()):
            cache[name] = frame = examples.run_example(
                {name: module}, name, environ=environ, benchmark_returns=returns)
            print('{}: {} rows, {} columns'.format(name, len(frame), len(frame.columns)))

        with tarfile.open(example_path, 'w|gz') as tar:
            tar.add(d.getpath('example_data'), 'example_data')


if __name__ == '__main__':
    main()
