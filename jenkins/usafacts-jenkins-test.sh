#!/usr/bin/env bash
#
# JHU: Jenkins test
#

set -exo pipefail
source ~/.bash_profile

#
# Test
#

local_indicator="usafacts"

cd "${WORKSPACE}/${local_indicator}" || exit

# Linter
#env/bin/pylint delphi_"${local_indicator}"
echo "Skip linting because we have weird breakage :( \
  TODO: https://github.com/cmu-delphi/covidcast-indicators/issues/333"

# Unit tests and code coverage
cd tests || exit && \
  ../env/bin/pytest --cov=delphi_"${local_indicator}" --cov-report=term-missing
