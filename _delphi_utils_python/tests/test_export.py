"""Tests for exporting CSV files."""
from datetime import datetime
from os import listdir, remove
from os.path import join

import pandas as pd
from delphi_utils import create_export_csv

def _clean_directory(directory):
    """Clean files out of a directory."""
    for fname in listdir(directory):
        if fname.startswith("."):
            continue
        remove(join(directory, fname))


def _non_ignored_files_set(directory):
    """List all files in a directory not preceded by a '.' and store them in a set."""
    out = set()
    for fname in listdir(directory):
        if fname.startswith("."):
            continue
        out.add(fname)
    return out


class TestExport:
    """Tests for exporting CSVs."""
    # List of times for data points.
    TIMES = [
        datetime.strptime(x, "%Y-%m-%d")
        for x in ["2020-02-15", "2020-02-15", "2020-03-01", "2020-03-15"]
    ]

    # A sample data frame.
    DF = pd.DataFrame(
        {
            "geo_id": ["51093", "51175", "51175", "51620"],
            "timestamp": TIMES,
            "val": [3.6, 2.1, 2.2, 2.6],
            "se": [0.15, 0.22, 0.20, 0.34],
            "sample_size": [100, 100, 101, 100],
        }
    )

    # Directory in which to store tests.
    TEST_DIR = "test_dir"

    def test_export_with_metric(self):
        """Test that exporting CSVs with the `metrics` argument yields the correct files."""

        # Clean receiving directory
        _clean_directory(self.TEST_DIR)

        create_export_csv(
            df=self.DF,
            start_date=datetime.strptime("2020-02-15", "%Y-%m-%d"),
            export_dir=self.TEST_DIR,
            metric="deaths",
            geo_res="county",
            sensor="test",
        )

        assert _non_ignored_files_set(self.TEST_DIR) == set(
            [
                "20200215_county_deaths_test.csv",
                "20200301_county_deaths_test.csv",
                "20200315_county_deaths_test.csv",
            ]
        )

    def test_export_without_metric(self):
        """Test that exporting CSVs without the `metrics` argument yields the correct files."""

        # Clean receiving directory
        _clean_directory(self.TEST_DIR)

        create_export_csv(
            df=self.DF,
            start_date=datetime.strptime("2020-02-15", "%Y-%m-%d"),
            export_dir=self.TEST_DIR,
            geo_res="county",
            sensor="test",
        )

        assert _non_ignored_files_set(self.TEST_DIR) == set(
            [
                "20200215_county_test.csv",
                "20200301_county_test.csv",
                "20200315_county_test.csv",
            ]
        )

    def test_export_with_limiting_start_date(self):
        """Test that the `start_date` prevents earlier dates from being exported."""

        # Clean receiving directory
        _clean_directory(self.TEST_DIR)

        create_export_csv(
            df=self.DF,
            start_date=datetime.strptime("2020-02-20", "%Y-%m-%d"),
            export_dir=self.TEST_DIR,
            geo_res="county",
            sensor="test",
        )

        assert _non_ignored_files_set(self.TEST_DIR) == set(
            [
                "20200301_county_test.csv",
                "20200315_county_test.csv",
            ]
        )

    def test_export_with_limiting_end_date(self):
        """Test that the `end_date` prevents later dates from being exported."""

        # Clean receiving directory
        _clean_directory(self.TEST_DIR)

        create_export_csv(
            df=self.DF,
            end_date=datetime.strptime("2020-03-07", "%Y-%m-%d"),
            export_dir=self.TEST_DIR,
            geo_res="county",
            sensor="test",
        )

        assert _non_ignored_files_set(self.TEST_DIR) == set(
            [
                "20200215_county_test.csv",
                "20200301_county_test.csv",
            ]
        )

    def test_export_with_no_dates(self):
        """Test that omitting the `start_date` and `end_date` exports all dates."""

        # Clean receiving directory
        _clean_directory(self.TEST_DIR)

        create_export_csv(
            df=self.DF,
            export_dir=self.TEST_DIR,
            geo_res="state",
            sensor="test",
        )

        assert _non_ignored_files_set(self.TEST_DIR) == set(
            [
                "20200215_state_test.csv",
                "20200301_state_test.csv",
                "20200315_state_test.csv",
            ]
        )

    def test_export_with_null_removal(self):
        """Test that `remove_null_samples = True` removes entries with null samples."""
        _clean_directory(self.TEST_DIR)

        df_with_nulls = self.DF.copy().append({
                                "geo_id": "66666",
                                "timestamp": datetime(2020, 6, 6),
                                "val": 10,
                                "se": 0.2,
                                "sample_size": pd.NA},
                            ignore_index=True)

        create_export_csv(
            df=df_with_nulls,
            export_dir=self.TEST_DIR,
            geo_res="state",
            sensor="test",
            remove_null_samples=True
        )

        assert _non_ignored_files_set(self.TEST_DIR) == set(
            [
                "20200215_state_test.csv",
                "20200301_state_test.csv",
                "20200315_state_test.csv",
                "20200606_state_test.csv"
            ]
        )
        assert pd.read_csv(join(self.TEST_DIR, "20200606_state_test.csv")).size == 0

    def test_export_without_null_removal(self):
        """Test that `remove_null_samples = False` does not remove entries with null samples."""
        _clean_directory(self.TEST_DIR)

        df_with_nulls = self.DF.copy().append({
                                "geo_id": "66666",
                                "timestamp": datetime(2020, 6, 6),
                                "val": 10,
                                "se": 0.2,
                                "sample_size": pd.NA},
                            ignore_index=True)

        create_export_csv(
            df=df_with_nulls,
            export_dir=self.TEST_DIR,
            geo_res="state",
            sensor="test",
            remove_null_samples=False
        )

        assert _non_ignored_files_set(self.TEST_DIR) == set(
            [
                "20200215_state_test.csv",
                "20200301_state_test.csv",
                "20200315_state_test.csv",
                "20200606_state_test.csv"
            ]
        )
        assert pd.read_csv(join(self.TEST_DIR, "20200606_state_test.csv")).size > 0
