#!/usr/bin/env bash
#
# JHU indicator Jenkins deploy
#

set -exo pipefail
source ~/.bash_profile

indicator="jhu"

#
# Deploy
#

cd "${WORKSPACE}/ansible" || exit

# Ansible!
ansbible-playbook "${indicator}"-ansible-deploy.yaml -i {{ midas_server }},