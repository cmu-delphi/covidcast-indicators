"""Tests for update_sensor.py."""
import os

from delphi_doctor_visits.update_sensor import update_sensor, write_to_csv

FILEPATH = "./test_data/SYNEDI_AGG_OUTPATIENT_07022020_1455CDT.csv.gz"
EXPORT_DIR = "./receiving"
COMPARISON_DIR = "./comparison/update_sensor"
WEEKDAY = False
SE = False
PREFIX = "foo"
GEO = "state"

class TestUpdateSensor:
    def test_update_sensor(self):
        for f in os.listdir(EXPORT_DIR):
            if f.startswith("."):
                continue
            os.remove(os.path.join(EXPORT_DIR, f))

        actual = update_sensor(
            filepath=FILEPATH,
            startdate="2020-02-04",
            enddate="2020-02-05",
            dropdate="2020-02-06",
            geo=GEO,
            parallel=True,
            weekday=WEEKDAY,
            se=SE
        )

        out_name = "smoothed_adj_cli" if WEEKDAY else "smoothed_cli"
        if SE:
            assert PREFIX is not None, "template has no obfuscated prefix"
            out_name = PREFIX + "_" + out_name

        write_to_csv(actual, GEO, SE, out_name, EXPORT_DIR)
        for fname in os.listdir(EXPORT_DIR):
            if fname.startswith("."):
                continue
            new = open(os.path.join(EXPORT_DIR, fname))
            old = open(os.path.join(COMPARISON_DIR, fname))
            assert new.readlines() == old.readlines()
