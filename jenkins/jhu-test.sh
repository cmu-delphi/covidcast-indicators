#!/usr/bin/env bash
#
# JHU indicator Jenkins tests
#

set -euxo pipefail

source ~/.bash_profile

cd "${WORKSPACE}"/jhu || exit

#
# Test
#

# Linter
env/bin/pylint delphi_jhu

# Unit tests and code coverage
cd tests || exit && \
  ../env/bin/pytest --cov=delphi_jhu --cov-report=term-missing
