"""What the tests that need a NATS server with JetStream share: whether there is one, and what a test does without.

By default such a test is skipped (no server at ``PLUTO_NATS_URL``, a server without JetStream, or ``nats-py`` not installed).
With ``PLUTO_REQUIRE_NATS=1`` (CI) every one of those is a failure instead: a green run then means the NATS tests ran.
"""
import os
import unittest

import pytest

REQUIRED = os.environ.get('PLUTO_REQUIRE_NATS') == '1'

try:
    from pluto.bus import jetstream
except ImportError as error:  # nats-py is not installed
    jetstream = None
    AVAILABLE = False
    REASON = 'nats-py is not installed ({})'.format(error)
else:
    AVAILABLE = jetstream.jetstream_available()
    REASON = 'no NATS server with JetStream at ' + jetstream.DEFAULT_URL


def require_module():
    """At the top of a test module that imports nats-py: skips the module without it (a failure when required)."""
    if jetstream is None:
        if REQUIRED:
            raise ImportError('PLUTO_REQUIRE_NATS=1 but ' + REASON)

        pytest.skip(REASON, allow_module_level=True)


def needs_server(case):
    """Class decorator: skip the tests of ``case`` without a NATS server with JetStream, or fail them when required."""
    if AVAILABLE:
        return case

    if REQUIRED:
        def setUpClass(cls):
            raise AssertionError('PLUTO_REQUIRE_NATS=1 but ' + REASON)

        case.setUpClass = classmethod(setUpClass)
        return case

    return unittest.skip(REASON)(case)
