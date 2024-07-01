"""Tests for update_sensor.py."""
import logging
import pandas as pd

from delphi_doctor_visits.update_sensor import update_sensor

TEST_LOGGER = logging.getLogger()

class TestProcessData:
    def test_csv_to_df(self):
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
