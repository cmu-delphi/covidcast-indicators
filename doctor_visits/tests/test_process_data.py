"""Tests for update_sensor.py."""
from datetime import datetime, timedelta
import logging
import os
from pathlib import Path
import pandas as pd

from delphi_doctor_visits.process_data import csv_to_df, write_to_csv, format_outname

TEST_LOGGER = logging.getLogger()

class TestProcessData:
    geo = "state",
    startdate = datetime(2020, 2, 4)
    enddate = datetime(2020, 2, 5)
    dropdate = datetime(2020, 2,6)
    geo = "state"
    se = False
    weekday = False
    prefix = "wip_XXXXX"
    filepath = "./test_data"
    compare_path = "./comparison"

    def test_csv_to_df(self):
        actual = csv_to_df(
            filepath=f"{self.filepath}/SYNEDI_AGG_OUTPATIENT_07022020_1455CDT.csv.gz",
            startdate=self.startdate,
            enddate=self.enddate,
            dropdate=self.dropdate,
            logger=TEST_LOGGER,
        )

        columns = list(actual.columns)
        expected = pd.read_pickle(f"{self.compare_path}/process_data/main_after_date_SYNEDI_AGG_OUTPATIENT_07022020_1455CDT.pkl")
        expected.reset_index(drop=True)
        expected = expected[columns]
        pd.testing.assert_frame_equal(expected, actual)

    def test_write_to_csv(self):
        output_df = pd.read_csv(f"{self.compare_path}/update_sensor/all.csv", parse_dates=["date"])

        write_to_csv(
            output_df=output_df,
            prefix=self.prefix,
            geo_level=self.geo,
            se=self.se,
            weekday=self.weekday,
            logger=TEST_LOGGER,
            output_path=self.filepath
        )

        outname = format_outname(self.prefix, self.se, self.weekday)

        files = list(Path(self.filepath).glob(f"*{outname}.csv"))

        for f in files:
            filename = f.name
            actual = pd.read_csv(f)
            expected = pd.read_csv(f"{self.compare_path}/process_data/{filename}")
            pd.testing.assert_frame_equal(expected, actual)
            os.remove(f)

