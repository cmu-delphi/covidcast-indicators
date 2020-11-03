#!/usr/bin/env bash
#
# Jenkins deploy
#

set -eo pipefail
source ~/.bash_profile

#
# Deploy
#

local_indicator=$1

cd "${WORKSPACE}/ansible" || exit

# Ansible!
ansible-playbook ansible-deploy-staging.yaml --extra-vars "indicator=${local_indicator}" -i inventory
