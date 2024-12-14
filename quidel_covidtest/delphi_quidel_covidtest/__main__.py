# -*- coding: utf-8 -*-
"""Call the function run_module when executed.

This file indicates that calling the module (`python -m MODULE_NAME`) will
call the function `run_module` found within the run.py file. There should be
no need to change this template.
"""

from delphi_utils import read_params
from .run import run_module  # pragma: no cover

run_module(read_params())  # pragma: no cover
