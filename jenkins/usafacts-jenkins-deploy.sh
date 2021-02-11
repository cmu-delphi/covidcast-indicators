#!/usr/bin/env bash
#
# Jenkins deploy
#

set -exo pipefail
source ~/.bash_profile

#
# Deploy
#

local_indicator="usafacts"

cd "${WORKSPACE}/ansible" || exit

# Ansible!
ansible-playbook ansible-deploy.yaml --extra-vars "indicator=${local_indicator}" -i inventory
