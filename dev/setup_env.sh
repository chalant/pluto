#!/bin/bash
# Create the Python 3.6 environment for Pluto's in-memory simulation path and
# build the Cython extensions in place.
#
#   dev/setup_env.sh [ENV_DIR]      (default: ~/.cache/pluto-dev/env36)
#
# Uses a static micromamba binary (downloaded next to ENV_DIR, no system
# packages, no root) for the compiled scientific stack from conda-forge, then
# pip for the pure-python packages. Versions are the ones the test path was
# verified with (see RESTORE_PROGRESS.md).
#
# Why Python 3.6: etc/requirements.txt pins pandas<=0.22 and the stored expected
# results (tests/resources/example_data.tar.gz) exist only for pandas 0.18.1,
# 0.19.2 and 0.22.0; pandas 0.22.0 has no binary for python >= 3.7.
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
ENV_DIR="${1:-$HOME/.cache/pluto-dev/env36}"
ROOT="$(dirname "$ENV_DIR")"
mkdir -p "$ROOT"

MM="$ROOT/bin/micromamba"
if [ ! -x "$MM" ]; then
    (cd "$ROOT" && curl -Ls https://micro.mamba.pm/api/micromamba/linux-64/latest | tar -xj bin/micromamba)
fi
export MAMBA_ROOT_PREFIX="$ROOT/mamba"

if [ ! -x "$ENV_DIR/bin/python" ]; then
    "$MM" create -y -p "$ENV_DIR" -c conda-forge --override-channels \
        python=3.6.15 numpy=1.16.6 pandas=0.22.0 cython=0.29.24 bcolz=1.2.1 \
        pytables=3.6.1 h5py=3.1.0 scipy=1.5.3 statsmodels=0.10.2 bottleneck=1.3.2 numexpr=2.7.3 \
        sqlalchemy=1.3.23 networkx=1.11 lru-dict=1.1.7 intervaltree=3.1.0 ta-lib=0.4.19 \
        grpcio=1.33.2 grpcio-tools=1.33.2 protobuf=3.18.0 pyyaml=5.4.1 toml=0.10.2 click=8.0.1 \
        logbook=1.5.3 toolz=0.12.0 contextlib2=21.6.0 six multipledispatch=0.6.0 pytz requests \
        python-dateutil patsy pip
fi

CONSTRAINTS="$ROOT/constraints-py36.txt"
cat > "$CONSTRAINTS" <<'EOF'
numpy==1.16.6
pandas==0.22.0
sqlalchemy==1.3.23
scipy==1.5.3
protobuf==3.18.0
grpcio==1.33.2
EOF

# alembic 1.8 and mako >= 1.2 need python >= 3.7; iso3166 >= 2 renamed TURKEY
# (zipline/country.py raises KeyError); tqdm, testfixtures, toml and
# clickhouse-driver are imported by the core path but not declared in
# etc/requirements.txt.
# empyrical 0.5.2: the stored expected results (alpha column) were computed with
# the linear annualisation mean(alpha)*252 of empyrical <= 0.5.2; 0.5.3+
# compounds ((1+mean)**252 - 1). See RESTORE_PROGRESS.md (M3).
"$ENV_DIR/bin/pip" install -c "$CONSTRAINTS" \
    alembic==1.7.7 mako==1.1.6 iso3166==1.0.1 iso4217 \
    trading-calendars==1.11.1 empyrical==0.5.2 python-interface==1.6.1 \
    tqdm==4.64.1 testfixtures==6.18.5 clickhouse-driver==0.2.5 ntplib \
    nose==1.3.7 nose-parameterized==0.6.0 parameterized==0.8.1 mock responses \
    pytest==7.0.1 "matplotlib<3.4"

(cd "$REPO" && "$ENV_DIR/bin/python" setup.py build_ext --inplace -j 8)
echo "environment ready: $ENV_DIR"
