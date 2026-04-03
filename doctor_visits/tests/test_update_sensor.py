"""Tests for update_sensor.py."""
from datetime import datetime
import logging
import pandas as pd

from delphi_doctor_visits.update_sensor import update_sensor
from delphi_doctor_visits.process_data import csv_to_df

TEST_LOGGER = logging.getLogger()

class TestUpdateSensor:
    start_date = datetime(2020, 2, 4)
    end_date = datetime(2020, 2, 5)
    drop_date = datetime(2020, 2, 6)
    def test_update_sensor(self):
        claims_df = csv_to_df(filepath="./test_data/SYNEDI_AGG_OUTPATIENT_07022020_1455CDT.csv.gz",
                              startdate=self.start_date, enddate=self.end_date, dropdate=self.drop_date,
                              logger=TEST_LOGGER)

        actual = update_sensor(
            data=claims_df,
            startdate=self.start_date,
            enddate=self.end_date,
            dropdate=self.drop_date,
            geo="state",
            parallel=False,
            weekday=False,
            se=False,
            logger=TEST_LOGGER,
        )

        comparison = pd.read_csv("./comparison/update_sensor/all.csv", parse_dates=["date"])
        pd.testing.assert_frame_equal(actual.reset_index(drop=True), comparison)
