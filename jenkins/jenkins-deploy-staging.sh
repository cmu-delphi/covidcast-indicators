#!/usr/bin/env bash
#
# Jenkins deploy
#

set -eox pipefail
source ~/.bash_profile

#
# Deploy
#

local_indicator=$1

echo "${local_indicator}"

cd "${WORKSPACE}/ansible" || exit

# Ansible!
ansible-playbook ansible-deploy-staging.yaml --extra-vars "indicator=${local_indicator}" -i inventory
