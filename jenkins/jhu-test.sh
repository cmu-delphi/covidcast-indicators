#!/usr/bin/env bash
#
# JHU indicator Jenkins tests
#

# Switch to working dir
cd "${WORKSPACE}"/jhu || exit

# Run soft linting
env/bin/pylint delphi_jhu

# TODO... more tests
