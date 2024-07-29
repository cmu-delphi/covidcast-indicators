#!/usr/bin/env bash
#
# Jenkins build-and-package
#

set -eo pipefail
source ~/.bash_profile

# Vars
local_indicator=$1

#
# Build
#

cd "${WORKSPACE}/${local_indicator}" || exit

# Set up venv
python -m venv env
source env/bin/activate
pip install pip==23.0.1 --retries 10 --timeout 20
pip install numpy --retries 10 --timeout 20
pip install ../_delphi_utils_python/. --retries 10 --timeout 20
[ ! -f setup.py ] || pip install . --retries 10 --timeout 20
