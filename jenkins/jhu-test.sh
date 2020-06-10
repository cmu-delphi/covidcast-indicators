#!/usr/bin/env bash
#
# JHU indicator Jenkins tests
#

# Source to learn about env
source ~/.bash_profile

# Switch to working dir
cd "${WORKSPACE}"/jhu || exit

# Test: Soft linting
env/bin/pylint delphi_jhu

# Test: Unit tests and code coverage
cd tests || exit && \
  ../env/bin/pytest --cov=delphi_jhu --cov-report=term-missing
