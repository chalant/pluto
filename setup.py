#!/usr/bin/env python
#
# Copyright 2014 Quantopian, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Modified by Pluto: the extension list is the one of zipline-reloaded 3.2.1
# (setup.py) on the flat package layout, plus Pluto's own extension; Pluto
# does not use versioneer or setuptools_scm.
from os.path import abspath, dirname, join

import numpy
from Cython.Build import cythonize
from setuptools import Extension, find_packages, setup

NO_DEPRECATED_API = [("NPY_NO_DEPRECATED_API", "NPY_1_7_API_VERSION")]


def window_specialization(typename):
    """Make an extension for an AdjustedArrayWindow specialization."""
    return Extension(
        name=f"zipline.lib._{typename}window",
        sources=[f"zipline/lib/_{typename}window.pyx"],
        depends=["zipline/lib/_windowtemplate.pxi"],
        define_macros=NO_DEPRECATED_API,
    )


def extension(name, source=None):
    return Extension(
        name=name,
        sources=[source or name.replace(".", "/") + ".pyx"],
        define_macros=NO_DEPRECATED_API,
    )


ext_options = dict(
    compiler_directives=dict(profile=True, language_level="3"),
)
ext_modules = [
    extension("zipline.assets._assets"),
    extension("zipline.assets.continuous_futures"),
    extension("zipline.lib.adjustment"),
    extension("zipline.lib._factorize"),
    window_specialization("float64"),
    window_specialization("int64"),
    window_specialization("uint8"),
    window_specialization("label"),
    extension("zipline.lib.rank"),
    extension("zipline.data._equities"),
    extension("zipline.data._adjustments"),
    extension("zipline._protocol"),
    extension("zipline.finance._finance_ext"),
    extension("zipline.gens.sim_engine"),
    extension("pluto.control.clock.sim_engine"),
    extension("zipline.data._minute_bar_internal"),
    extension("zipline.data._resample"),
]


def read_requirements(path):
    """Read a requirements file, expressed as a path relative to the root."""
    with open(join(dirname(abspath(__file__)), path)) as f:
        lines = (line.strip() for line in f)
        return [line for line in lines if line and not line.startswith("#")]


setup(
    name="pluto",
    url="https://github.com/chalant/zipline-core",
    version="0.1.0.dev0",
    description="A backtester and live trader for financial algorithms.",
    entry_points={
        "console_scripts": [
            "zipline = zipline.__main__:main",
        ],
    },
    author="chalant",
    packages=find_packages(include=["pluto", "pluto.*", "zipline", "zipline.*"]),
    ext_modules=cythonize(ext_modules, force=True, **ext_options),
    include_dirs=[numpy.get_include()],
    include_package_data=True,
    license="Apache-2.0",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "License :: OSI Approved :: Apache Software License",
        "Natural Language :: English",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.13",
        "Operating System :: OS Independent",
        "Intended Audience :: Science/Research",
        "Topic :: Office/Business :: Financial",
        "Topic :: Scientific/Engineering :: Information Analysis",
        "Topic :: System :: Distributed Computing",
    ],
    python_requires=">=3.13,<3.14",
    install_requires=read_requirements("etc/requirements.txt"),
)
