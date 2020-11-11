"""Tests for exporting CSV files."""
from datetime import datetime
from os import listdir, remove
from os.path import join

import pandas as pd
from delphi_utils import create_export_csv


class TestExport:
    # List of times for data points.
    TIMES = [
        datetime.strptime(x, "%Y-%m-%d")
        for x in ["2020-02-15", "2020-02-15", "2020-03-01", "2020-03-15"]
    ]

    # A sample data frame
    DF = pd.DataFrame(
        {
            "geo_id": ["51093", "51175", "51175", "51620"],
            "timestamp": TIMES,
            "val": [3.6, 2.1, 2.2, 2.6],
            "se": [0.15, 0.22, 0.20, 0.34],
            "sample_size": [100, 100, 101, 100],
        }
    )

    def test_export_with_metric(self):
        """Test that exporting CSVs with the `metrics` argument yields the correct files."""

        # Clean receiving directory
        for fname in listdir("test_dir"):
            remove(join("test_dir", fname))

        create_export_csv(
            df=self.DF,
            start_date=datetime.strptime("2020-02-15", "%Y-%m-%d"),
            export_dir="test_dir",
            metric="deaths",
            geo_res="county",
            sensor="test",
        )

        assert set(listdir("test_dir")) == set(
            [
                "20200215_county_deaths_test.csv",
                "20200301_county_deaths_test.csv",
                "20200315_county_deaths_test.csv",
            ]
        )

    def test_export_without_metric(self):
        """Test that exporting CSVs without the `metrics` argument yields the correct files."""

        # Clean receiving directory
        for fname in listdir("test_dir"):
            remove(join("test_dir", fname))

        create_export_csv(
            df=self.DF,
            start_date=datetime.strptime("2020-02-15", "%Y-%m-%d"),
            export_dir="test_dir",
            geo_res="county",
            sensor="test",
        )

        assert set(listdir("test_dir")) == set(
            [
                "20200215_county_test.csv",
                "20200301_county_test.csv",
                "20200315_county_test.csv",
            ]
        )

    def test_export_with_limiting_start_date(self):
        """Test that the `start_date` prevents earlier dates from being exported."""

        # Clean receiving directory
        for fname in listdir("test_dir"):
            remove(join("test_dir", fname))

        create_export_csv(
            df=self.DF,
            start_date=datetime.strptime("2020-02-20", "%Y-%m-%d"),
            export_dir="test_dir",
            geo_res="county",
            sensor="test",
        )

        assert set(listdir("test_dir")) == set(
            [
                "20200301_county_test.csv",
                "20200315_county_test.csv",
            ]
        )

    def test_export_with_no_start_date(self):
        """Test that omitting the `start_date` exports all dates."""

        # Clean receiving directory
        for fname in listdir("test_dir"):
            remove(join("test_dir", fname))

        create_export_csv(
            df=self.DF,
            export_dir="test_dir",
            geo_res="state",
            sensor="test",
        )

        assert set(listdir("test_dir")) == set(
            [
                "20200215_state_test.csv",
                "20200301_state_test.csv",
                "20200315_state_test.csv",
            ]
        )
