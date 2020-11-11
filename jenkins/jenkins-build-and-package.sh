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
pip install ../_delphi_utils_python/.
pip install .

#
# Package
#

cd "${WORKSPACE}" || exit

# Create .tar.gz for deployment
# TODO Rename test-artifacts to artifacts when this works!!!
tar -czvf "${JENKINS_HOME}/test-artifacts/${local_indicator}.tar.gz" "${local_indicator}"
