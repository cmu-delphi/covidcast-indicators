"""Tests for exporting CSV files."""
from datetime import datetime
from os import listdir, remove
from os.path import join

import mock
import numpy as np
import pandas as pd

from delphi_utils import create_export_csv, Nans


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
            "val": [3.12345678910, 2.1, 2.2, 2.6],
            "se": [0.15, 0.22, 0.20, 0.34],
            "sample_size": [100, 100, 101, 100],
        }
    )

    # A sample data frame with missingness.
    DF2 = pd.DataFrame(
        {
            "geo_id": ["51093", "51175", "51175", "51620"],
            "timestamp": TIMES,
            "val": [3.12345678910, np.nan, 2.2, 2.6],
            "se": [0.15, 0.22, np.nan, 0.34],
            "sample_size": [100, 100, 101, None],
            "missing_val": [
                Nans.NOT_MISSING,
                Nans.OTHER,
                Nans.NOT_MISSING,
                Nans.NOT_MISSING,
            ],
            "missing_se": [
                Nans.NOT_MISSING,
                Nans.NOT_MISSING,
                Nans.OTHER,
                Nans.NOT_MISSING,
            ],
            "missing_sample_size": [Nans.NOT_MISSING] * 3 + [Nans.OTHER],
        }
    )

    # A sample data frame with contradictory missing codes.
    DF3 = pd.DataFrame(
        {
            "geo_id": ["51093", "51175", "51175", "51620"],
            "timestamp": TIMES,
            "val": [np.nan, np.nan, 2.2, 2.6],
            "se": [0.15, 0.22, np.nan, 0.34],
            "sample_size": [100, 100, 101, None],
            "missing_val": [
                Nans.NOT_MISSING,
                Nans.OTHER,
                Nans.NOT_MISSING,
                Nans.NOT_MISSING,
            ],
            "missing_se": [
                Nans.NOT_MISSING,
                Nans.NOT_MISSING,
                Nans.OTHER,
                Nans.NOT_MISSING,
            ],
            "missing_sample_size": [Nans.NOT_MISSING] * 3 + [Nans.OTHER],
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

    def test_export_rounding(self):
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
        pd.testing.assert_frame_equal(
            pd.read_csv(join(self.TEST_DIR, "20200215_county_deaths_test.csv")),
            pd.DataFrame(
                {
                    "geo_id": [51093, 51175],
                    "val": [round(3.12345678910, 7), 2.1],
                    "se": [0.15, 0.22],
                    "sample_size": [100, 100],
                }
            ),
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

        df_with_nulls = self.DF.copy().append(
            {
                "geo_id": "66666",
                "timestamp": datetime(2020, 6, 6),
                "val": 10,
                "se": 0.2,
                "sample_size": pd.NA,
            },
            ignore_index=True,
        )

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

        df_with_nulls = self.DF.copy().append(
            {
                "geo_id": "66666",
                "timestamp": datetime(2020, 6, 6),
                "val": 10,
                "se": 0.2,
                "sample_size": pd.NA,
            },
            ignore_index=True,
        )

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

    def test_export_df_without_missingness(self):
        _clean_directory(self.TEST_DIR)

        create_export_csv(
            df=self.DF.copy(), export_dir=self.TEST_DIR, geo_res="county", sensor="test"
        )
        df = pd.read_csv(join(self.TEST_DIR, "20200215_county_test.csv")).astype(
            {"geo_id": str, "sample_size": int}
        )
        expected_df = pd.DataFrame(
            {
                "geo_id": ["51093", "51175"],
                "val": [3.12345678910, 2.1],
                "se": [0.15, 0.22],
                "sample_size": [100, 100],
            }
        ).astype({"geo_id": str, "sample_size": int})
        pd.testing.assert_frame_equal(df, expected_df)

    def test_export_df_with_missingness(self):
        _clean_directory(self.TEST_DIR)

        create_export_csv(
            df=self.DF2.copy(),
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
        df = pd.read_csv(join(self.TEST_DIR, "20200215_county_test.csv")).astype(
            {"geo_id": str, "sample_size": int}
        )
        expected_df = pd.DataFrame(
            {
                "geo_id": ["51093", "51175"],
                "val": [3.12345678910, np.nan],
                "se": [0.15, 0.22],
                "sample_size": [100, 100],
                "missing_val": [Nans.NOT_MISSING, Nans.OTHER],
                "missing_se": [Nans.NOT_MISSING] * 2,
                "missing_sample_size": [Nans.NOT_MISSING] * 2,
            }
        ).astype({"geo_id": str, "sample_size": int})
        pd.testing.assert_frame_equal(df, expected_df)

    @mock.patch("delphi_utils.logger")
    def test_export_df_with_contradictory_missingness(self, mock_logger):
        _clean_directory(self.TEST_DIR)

        create_export_csv(
            df=self.DF3.copy(),
            export_dir=self.TEST_DIR,
            geo_res="state",
            sensor="test",
            logger=mock_logger
        )
        assert _non_ignored_files_set(self.TEST_DIR) == set(
            [
                "20200215_state_test.csv",
                "20200301_state_test.csv",
                "20200315_state_test.csv",
            ]
        )
        assert pd.read_csv(join(self.TEST_DIR, "20200315_state_test.csv")).size > 0
        mock_logger.info.assert_called_once_with(
            "Filtering contradictory missing code in test_None_2020-02-15."
        )
