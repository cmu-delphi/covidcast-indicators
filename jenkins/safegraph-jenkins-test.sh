#!/usr/bin/env bash
#
# JHU: Jenkins test
#

set -eo pipefail
source ~/.bash_profile

#
# Test
#

local_indicator="safegraph"

cd "${WORKSPACE}/${local_indicator}" || exit

# Linter
env/bin/pylint --disable=C --disable=W --disable=R delphi_"${local_indicator}"

# Unit tests and code coverage
cd tests || exit && \
  ../env/bin/pytest --cov=delphi_"${local_indicator}" --cov-report=term-missing
