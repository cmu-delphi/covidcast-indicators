#!/usr/bin/env bash
#
# JHU indicator test routine
#

# Switch to working dir
cd $WORKSPACE/jhu || exit

# Set up venv
python -m venv env
source env/bin/activate
pip install ../_delphi_utils_python/.
pip install .

# Run soft linting
env/bin/pylint delphi_jhu

# TODO... more tests
