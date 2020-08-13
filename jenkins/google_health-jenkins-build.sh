#!/usr/bin/env bash
#
# JHU: Jenkins build
#

set -eo pipefail
source ~/.bash_profile

#
# Build
#

local_indicator="google_health"

cd "${WORKSPACE}/${local_indicator}" || exit

# Set up venv
python -m venv env
source env/bin/activate
pip install ../_delphi_utils_python/.
pip install .

# Ansible!
# We need to call some extra Ansible here to handle placing a special params.json
# template so that our tests can complete. The below calls a small playbook that
# runs locally on the build (Jenkins) server to place the file.

cd "${WORKSPACE}/ansible" || exit

ansible-playbook google_health-build.yaml \
  --extra-vars "indicator=${local_indicator} workspace=${WORKSPACE}" -i localhost,