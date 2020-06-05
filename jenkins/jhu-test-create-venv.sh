#!/usr/bin/env bash
#
# Build venv
#

cd jhu

python -m venv env
source env/bin/activate
pip install ../_delphi_utils_python/.
pip install .
