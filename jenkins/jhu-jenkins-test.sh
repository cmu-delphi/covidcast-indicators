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
env/bin/pylint delphi_jhu

# Unit tests and code coverage
cd tests || exit && \
  ../env/bin/pytest --cov=delphi_jhu --cov-report=term-missing
