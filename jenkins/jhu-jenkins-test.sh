#!/usr/bin/env bash
#
# JHU indicator: Jenkins test
#

set -exo pipefail
source ~/.bash_profile

#
# Test
#

indicator="jhu"

cd "${WORKSPACE}/${indicator}" || exit

# Linter
env/bin/pylint delphi_jhu

# Unit tests and code coverage
cd tests || exit && \
  ../env/bin/pytest --cov=delphi_jhu --cov-report=term-missing
