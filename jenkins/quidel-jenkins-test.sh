#!/usr/bin/env bash
#
# Jenkins test
#

set -eo pipefail
source ~/.bash_profile

#
# Test
#

local_indicator="quidel_covidtest"

cd "${WORKSPACE}/${local_indicator}" || exit

# Linter
env/bin/pylint delphi_"${local_indicator}"

# Unit tests and code coverage
cd tests || exit && \
  ../env/bin/pytest --cov=delphi_"${local_indicator}" --cov-report=term-missing
