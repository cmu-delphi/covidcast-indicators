"""Tests for update_sensor.py."""
from datetime import datetime
import logging
import pandas as pd

from delphi_doctor_visits.update_sensor import update_sensor

TEST_LOGGER = logging.getLogger()

class TestUpdateSensor:
    def test_update_sensor(self):
        df = pd.read_pickle("./test_data/SYNEDI_AGG_OUTPATIENT_07022020_1455CDT.pkl")
        actual = update_sensor(
            data=df,
            startdate=datetime(2020, 2, 4),
            enddate=datetime(2020, 2, 5),
            dropdate=datetime(2020, 2,6),
            geo="state",
            parallel=False,
            weekday=False,
            se=False,
            logger=TEST_LOGGER,
        )

        comparison = pd.read_csv("./comparison/update_sensor/all.csv", parse_dates=["date"])
        pd.testing.assert_frame_equal(actual.reset_index(drop=True), comparison)
