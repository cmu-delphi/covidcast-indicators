"""Tests for update_sensor.py."""
from datetime import datetime
import logging
import pandas as pd

from delphi_doctor_visits.process_data import csv_to_df

TEST_LOGGER = logging.getLogger()

class TestProcessData:
    def test_csv_to_df(self):
        actual = csv_to_df(
            filepath="./test_data/SYNEDI_AGG_OUTPATIENT_07022020_1455CDT.csv.gz",
            startdate=datetime(2020, 2, 4),
            enddate=datetime(2020, 2, 5),
            dropdate=datetime(2020, 2,6),
            logger=TEST_LOGGER,
        )

        comparison = pd.read_pickle("./comparison/process_data/main_after_date_SYNEDI_AGG_OUTPATIENT_07022020_1455CDT.pkl")
        pd.testing.assert_frame_equal(actual.reset_index(drop=True), comparison)
