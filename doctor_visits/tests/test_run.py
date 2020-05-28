import pytest

import pandas as pd

from delphi_doctor_visits.run import run_module


class TestRun:
    def test_run(self):

        run_module()
