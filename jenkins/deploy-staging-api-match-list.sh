#!/usr/bin/env bash
#
# Jenkins deploy staging api match list
#

set -eo pipefail
source ~/.bash_profile

#
# Deploy
#

cd "${WORKSPACE}/ansible" || exit

# Ansible!
ansible-playbook ansible-deploy-staging-api-proxy-match-list.yaml -i inventory
