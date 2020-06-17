#!/usr/bin/env bash
#
# Jenkins package
#

set -exo pipefail
source ~/.bash_profile

#
# Package
#

local_indicator="jhu"

cd "${WORKSPACE}" || exit

# Create .tar.gz for deployment
tar -czvf "${JENKINS_HOME}/artifacts/${local_indicator}.tar.gz" "${local_indicator}"
