#!/usr/bin/env bash
#
# Build venv
#

python -m venv env
source env/bin/activate
pip install _delphi_utils_python/.
pip install .
