#!/usr/bin/env bash
#
# Jenkins deploy
#

set -eo pipefail
source ~/.bash_profile

#
# Deploy
#

local_indicator="quidel_covidtest"

cd "${WORKSPACE}/ansible" || exit

# Ansible!
ansible-playbook ansible-deploy.yaml --extra-vars "indicator=${local_indicator}" -i inventory
