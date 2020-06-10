#!/usr/bin/env bash
#
# JHU indicators Jenkins package
#

set -exo pipefail
source ~/.bash_profile

indicator="jhu"

#
# Package
#

cd "${WORKSPACE}" || exit

# Create .tar.gz for later deployment
# tar -czvf "${WORKSPACE}/${indicator}.tar.gz" "${indicator}"
 tar -czvf "../${indicator}.tar.gz" "${indicator}"
