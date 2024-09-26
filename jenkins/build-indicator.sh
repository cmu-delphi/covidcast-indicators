#!/usr/bin/env bash
#
# Jenkins build
#

set -eo pipefail
source ~/.bash_profile

# Vars
local_indicator=$1

cd "${WORKSPACE}/${local_indicator}" || exit

# Set up venv
python -m venv env
source env/bin/activate
pip install pip==23.0.1 --retries 10 --timeout 20
pip install numpy --retries 10 --timeout 20
pip install ../_delphi_utils_python/. --retries 10 --timeout 20
if [ -f setup.py ] || [ -f pyproject.toml ]; then
    pip install . --retries 10 --timeout 20
fi
