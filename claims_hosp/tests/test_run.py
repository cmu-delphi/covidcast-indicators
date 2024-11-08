import logging
from os import listdir
from os.path import join
from itertools import product

import pandas as pd
import pytest

from conftest import TEST_DIR
class TestRun:
    @pytest.mark.freeze_time("2020-11-07")
    def test_output_files_exist(self, run_as_module):
        pass
