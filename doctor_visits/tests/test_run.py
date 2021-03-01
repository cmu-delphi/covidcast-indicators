"""Tests for run.py."""
import os

from delphi_doctor_visits.run import run_module

EXPORT_DIR = "./receiving"
COMPARISON_DIR = "./comparison/run"

class TestRun:
    PARAMS = {
        "common": {
            "export_dir": "./receiving"
        },
        "indicator": {
            "input_file": "./test_data/SYNEDI_AGG_OUTPATIENT_07022020_1455CDT.csv.gz",
            "drop_date": "",
            "n_backfill_days": 10,
            "n_waiting_days": 2,
            "weekday": [False],
            "se": False,
            "obfuscated_prefix": "wip_XXXXX",
            "parallel": False
        }
    }

    def test_run(self):
        for f in os.listdir(EXPORT_DIR):
            if f.startswith("."):
                continue
            os.remove(os.path.join(EXPORT_DIR, f))

        run_module(self.PARAMS)
        for fname in os.listdir(COMPARISON_DIR):
            new = open(os.path.join(EXPORT_DIR, fname))
            old = open(os.path.join(COMPARISON_DIR, fname))
            assert new.readlines() == old.readlines()
