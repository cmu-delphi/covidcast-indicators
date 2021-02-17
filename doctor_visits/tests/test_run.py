import pytest

import pandas as pd

from delphi_doctor_visits.run import run_module


class TestRun:
    PARAMS = {
        "common": {
            "export_dir": "./receiving"
        },
        "indicator": {
            "input_file": "./test_data/SYNEDI_AGG_OUTPATIENT_07022020_1455CDT.csv.gz",
            "drop_date": "",
            "n_backfill_days": 60,
            "n_waiting_days": 3,
            "weekday": [True, False],
            "se": False,
            "obfuscated_prefix": "wip_XXXXX",
            "parallel": False
        }
    }

    def todo_test_run(self):
        run_module(self.PARAMS)
