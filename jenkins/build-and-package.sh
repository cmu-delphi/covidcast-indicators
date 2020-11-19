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
pip install --upgrade pip --retries 10 --timeout 20
pip install ../_delphi_utils_python/. --retries 10 --timeout 20
pip install . --retries 10 --timeout 20

#
# Package
#

cd "${WORKSPACE}" || exit

# Create .tar.gz for deployment
tar -czvf "${JENKINS_HOME}/artifacts/${local_indicator}.tar.gz" "${local_indicator}"
