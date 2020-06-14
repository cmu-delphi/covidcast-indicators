#!/usr/bin/env bash
#
# Jenkins deploy
#

set -exo pipefail
source ~/.bash_profile

#
# Deploy
#

cd "${WORKSPACE}/ansible" || exit

# Ansible!
ansible-playbook ansible-deploy.yaml --extra-vars "indicator=${INDICATOR}" -i inventory
