#!/usr/bin/env bash
#
# Jenkins package
#

set -eo pipefail
source ~/.bash_profile

#
# Package
#

local_indicator="google_health"

cd "${WORKSPACE}" || exit

# Create .tar.gz for deployment
tar -czvf "${JENKINS_HOME}/artifacts/${local_indicator}.tar.gz" "${local_indicator}"
