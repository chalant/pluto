#!/bin/bash
# Create the Python 3.13 environment for Pluto and build the Cython extensions in place.
#
#   dev/setup_env.sh [ENV_DIR]      (default: ~/.cache/pluto-dev/env313)
#
# Needs `uv` (https://docs.astral.sh/uv/) and a C compiler (bcolz-zipline and the
# extensions are built from source). uv downloads a managed Python 3.13 if there is
# none; the tests were run with 3.13.16 (PYTHON_VERSION=3.13.16 pins it).
# The packages are the exact versions of etc/requirements-py313.txt (`uv pip sync`
# installs exactly that set and removes anything else). To refresh the pins, edit
# that file, they are not resolved here.
#
# Python 3.6 history: the environment of branch restore-sim (Python 3.6, pandas 0.22,
# micromamba) is described in RESTORE_PROGRESS.md; this script no longer creates it.
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
ENV_DIR="${1:-$HOME/.cache/pluto-dev/env313}"
PYTHON_VERSION="${PYTHON_VERSION:-3.13}"

command -v uv >/dev/null || { echo "uv is required: https://docs.astral.sh/uv/" >&2; exit 1; }

if [ ! -x "$ENV_DIR/bin/python" ]; then
    uv venv --python "$PYTHON_VERSION" "$ENV_DIR"
fi

uv pip sync --python "$ENV_DIR/bin/python" "$REPO/etc/requirements-py313.txt"

(cd "$REPO" && "$ENV_DIR/bin/python" setup.py build_ext --inplace -j 8)
echo "environment ready: $ENV_DIR"
