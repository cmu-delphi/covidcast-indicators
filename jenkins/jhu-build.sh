#!/usr/bin/env bash
#
# JHU indicator Jenkins build
#

# Switch to working dir
cd $WORKSPACE/jhu || exit

# Set up venv
python -m venv env
source env/bin/activate
pip install ../_delphi_utils_python/.
pip install .
