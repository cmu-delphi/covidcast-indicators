#!/usr/bin/env bash
#
# Change HC: Jenkins deploy
#

set -eo pipefail
source ~/.bash_profile

#
# Deploy
#

local_indicator="changehc"

cd "${WORKSPACE}/ansible" || exit

# Ansible!
ansible-playbook ansible-deploy.yaml --extra-vars "indicator=${local_indicator}" -i inventory
