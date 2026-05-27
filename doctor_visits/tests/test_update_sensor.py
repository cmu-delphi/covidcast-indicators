"""Tests for update_sensor.py."""
import logging
import gzip
import shutil
import pandas as pd
import pytest

from delphi_doctor_visits.update_sensor import update_sensor

TEST_LOGGER = logging.getLogger()

class TestUpdateSensor:
    def test_update_sensor(self):
        actual = update_sensor(
            filepath="./test_data/SYNEDI_AGG_OUTPATIENT_07022020_1455CDT.csv.gz",
            startdate="2020-02-04",
            enddate="2020-02-05",
            dropdate="2020-02-06",
            geo="state",
            parallel=False,
            weekday=False,
            se=False,
            logger=TEST_LOGGER,
        )

        comparison = pd.read_csv("./comparison/update_sensor/all.csv", parse_dates=["date"])
        pd.testing.assert_frame_equal(actual.reset_index(drop=True), comparison)

    def test_update_sensor_drops_empty_fips(self, tmp_path):
        """Rows with empty PatCountyFIPS must be silently dropped."""
        from delphi_doctor_visits.config import Config

        src = "./test_data/SYNEDI_AGG_OUTPATIENT_07022020_1455CDT.csv.gz"
        # Read with Config.DTYPES to preserve zero-padded FIPS strings
        data = pd.read_csv(src, dtype=Config.DTYPES)

        # append rows with no location info (empty FIPS and HRR fields)
        empty_rows = data.head(3).copy()
        empty_rows["PatCountyFIPS"] = ""
        empty_rows["Pat HRR Name"] = ""
        empty_rows["Pat HRR ID"] = ""
        augmented = pd.concat([data, empty_rows], ignore_index=True)

        out_path = tmp_path / "augmented.csv.gz"
        with gzip.open(out_path, "wt") as fh:
            augmented.to_csv(fh, index=False)

        actual = update_sensor(
            filepath=str(out_path),
            startdate="2020-02-04",
            enddate="2020-02-05",
            dropdate="2020-02-06",
            geo="state",
            parallel=False,
            weekday=False,
            se=False,
            logger=TEST_LOGGER,
        )

        comparison = pd.read_csv("./comparison/update_sensor/all.csv", parse_dates=["date"])
        pd.testing.assert_frame_equal(actual.reset_index(drop=True), comparison)
