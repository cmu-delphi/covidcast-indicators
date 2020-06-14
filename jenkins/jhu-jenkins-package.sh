#!/usr/bin/env bash
#
# JHU indicator: Jenkins package
#

set -exo pipefail
source ~/.bash_profile

#
# Package
#

#indicator="jhu"

cd "${WORKSPACE}" || exit

# Create .tar.gz for deployment
 tar -czvf "${JENKINS_HOME}/artifacts/${INDICATOR}.tar.gz" "${INDICATOR}"
