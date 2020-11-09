#!/usr/bin/env bash
#
# JHU: Jenkins build
#

set -exo pipefail
source ~/.bash_profile

#
# Build
#

local_indicator="jhu"

cd "${WORKSPACE}/${local_indicator}" || exit

# Set up venv
python -m venv env
source env/bin/activate
pip install ../_delphi_utils_python/.
pip install .
