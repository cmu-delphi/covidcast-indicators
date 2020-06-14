#!/usr/bin/env bash
#
# Jenkins package
#

set -exo pipefail
source ~/.bash_profile

#
# Package
#

cd "${WORKSPACE}" || exit

# Create .tar.gz for deployment
 tar -czvf "${JENKINS_HOME}/artifacts/${INDICATOR}.tar.gz" "${INDICATOR}"
