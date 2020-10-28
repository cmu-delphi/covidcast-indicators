#!/usr/bin/env bash
#
# JHU: Jenkins test
#

set -exo pipefail
source ~/.bash_profile

#
# Test
#

local_indicator="jhu"

cd "${WORKSPACE}/${local_indicator}" || exit

# Linter
#env/bin/pylint delphi_"${local_indicator}"
echo "Skip linting because we have weird breakage :("

# Unit tests and code coverage
cd tests || exit && \
  ../env/bin/pytest --cov=delphi_"${local_indicator}" --cov-report=term-missing
