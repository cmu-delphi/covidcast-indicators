#!/usr/bin/env bash
#
# Jenkins build
#

set -exo pipefail
source ~/.bash_profile

#
# Build
#

local_indicator="quidel"

cd "${WORKSPACE}/${local_indicator}" || exit

# Set up venv
python -m venv env
source env/bin/activate
pip install ../_delphi_utils_python/.
pip install .
