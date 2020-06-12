#!/usr/bin/env bash
#
# JHU indicator: Jenkins build
#

set -exo pipefail
source ~/.bash_profile

#
# Build
#

indicator="jhu"

cd "${WORKSPACE}/${indicator}" || exit

# Set up venv
python -m venv env
source env/bin/activate
pip install ../_delphi_utils_python/.
pip install .
