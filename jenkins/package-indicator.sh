#!/usr/bin/env bash
#
# Jenkins build-and-package
#

set -eo pipefail
source ~/.bash_profile

# Vars
local_indicator=$1
branch=$2

#
# Package
#

cd "${WORKSPACE}" || exit

# Create .tar.gz for deployment
tar -czvf "${JENKINS_HOME}/artifacts/${branch}_${local_indicator}.tar.gz" "${local_indicator}"
