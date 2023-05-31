"""Tests for exporting CSV files."""
from datetime import datetime
from os import listdir
from os.path import join
from typing import Any, Dict, List

import mock
import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal

from delphi_utils import create_export_csv, Nans


def _set_df_dtypes(df: pd.DataFrame, dtypes: Dict[str, Any]) -> pd.DataFrame:
    assert all(isinstance(e, type) or isinstance(e, str) for e in dtypes.values()), (
        "Values must be types or Pandas string aliases for types."
    )

    df = df.copy()
    for k, v in dtypes.items():
        if k in df.columns:
            df[k] = df[k].astype(v)
    return df


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

    def test_export_with_metric(self, tmp_path):
        """Test that exporting CSVs with the `metrics` argument yields the correct files."""

        create_export_csv(
            df=self.DF,
            start_date=datetime.strptime("2020-02-15", "%Y-%m-%d"),
            export_dir=tmp_path,
            metric="deaths",
            geo_res="county",
            sensor="test",
        )

        assert set(listdir(tmp_path)) == set(
            [
                "20200215_county_deaths_test.csv",
                "20200301_county_deaths_test.csv",
                "20200315_county_deaths_test.csv",
            ]
        )

    def test_export_rounding(self, tmp_path):
        """Test that exporting CSVs with the `metrics` argument yields the correct files."""

        create_export_csv(
            df=self.DF,
            start_date=datetime.strptime("2020-02-15", "%Y-%m-%d"),
            export_dir=tmp_path,
            metric="deaths",
            geo_res="county",
            sensor="test",
        )
        assert_frame_equal(
            pd.read_csv(join(tmp_path, "20200215_county_deaths_test.csv")),
            pd.DataFrame(
                {
                    "geo_id": [51093, 51175],
                    "val": [round(3.12345678910, 7), 2.1],
                    "se": [0.15, 0.22],
                    "sample_size": [100, 100],
                }
            ),
        )

    def test_export_without_metric(self, tmp_path):
        """Test that exporting CSVs without the `metrics` argument yields the correct files."""

        create_export_csv(
            df=self.DF,
            start_date=datetime.strptime("2020-02-15", "%Y-%m-%d"),
            export_dir=tmp_path,
            geo_res="county",
            sensor="test",
        )

        assert set(listdir(tmp_path)) == set(
            [
                "20200215_county_test.csv",
                "20200301_county_test.csv",
                "20200315_county_test.csv",
            ]
        )

    def test_export_with_limiting_start_date(self, tmp_path):
        """Test that the `start_date` prevents earlier dates from being exported."""

        create_export_csv(
            df=self.DF,
            start_date=datetime.strptime("2020-02-20", "%Y-%m-%d"),
            export_dir=tmp_path,
            geo_res="county",
            sensor="test",
        )

        assert set(listdir(tmp_path)) == set(
            [
                "20200301_county_test.csv",
                "20200315_county_test.csv",
            ]
        )

    def test_export_with_limiting_end_date(self, tmp_path):
        """Test that the `end_date` prevents later dates from being exported."""


        create_export_csv(
            df=self.DF,
            end_date=datetime.strptime("2020-03-07", "%Y-%m-%d"),
            export_dir=tmp_path,
            geo_res="county",
            sensor="test",
        )

        assert set(listdir(tmp_path)) == set(
            [
                "20200215_county_test.csv",
                "20200301_county_test.csv",
            ]
        )

    def test_export_with_no_dates(self, tmp_path):
        """Test that omitting the `start_date` and `end_date` exports all dates."""

        create_export_csv(
            df=self.DF,
            export_dir=tmp_path,
            geo_res="state",
            sensor="test",
        )

        assert set(listdir(tmp_path)) == set(
            [
                "20200215_state_test.csv",
                "20200301_state_test.csv",
                "20200315_state_test.csv",
            ]
        )

    def test_export_with_null_removal(self, tmp_path):
        """Test that `remove_null_samples = True` removes entries with null samples."""

        df_with_nulls = pd.concat(
            [self.DF.copy(),
            pd.DataFrame({
                "geo_id": "66666",
                "timestamp": datetime(2020, 6, 6),
                "val": 10,
                "se": 0.2,
                "sample_size": pd.NA,
            }, index = [0])]
        )

        create_export_csv(
            df=df_with_nulls,
            export_dir=tmp_path,
            geo_res="state",
            sensor="test",
            remove_null_samples=True
        )

        assert set(listdir(tmp_path)) == set(
            [
                "20200215_state_test.csv",
                "20200301_state_test.csv",
                "20200315_state_test.csv",
                "20200606_state_test.csv"
            ]
        )
        assert pd.read_csv(join(tmp_path, "20200606_state_test.csv")).size == 0

    def test_export_without_null_removal(self, tmp_path):
        """Test that `remove_null_samples = False` does not remove entries with null samples."""
        df_with_nulls = pd.concat(
            [self.DF.copy(),
            pd.DataFrame({
                "geo_id": "66666",
                "timestamp": datetime(2020, 6, 6),
                "val": 10,
                "se": 0.2,
                "sample_size": pd.NA,
            }, index = [0])]
        )

        create_export_csv(
            df=df_with_nulls,
            export_dir=tmp_path,
            geo_res="state",
            sensor="test",
            remove_null_samples=False
        )

        assert set(listdir(tmp_path)) == set(
            [
                "20200215_state_test.csv",
                "20200301_state_test.csv",
                "20200315_state_test.csv",
                "20200606_state_test.csv"
            ]
        )
        assert pd.read_csv(join(tmp_path, "20200606_state_test.csv")).size > 0

    def test_export_df_without_missingness(self, tmp_path):

        create_export_csv(
            df=self.DF.copy(), export_dir=tmp_path, geo_res="county", sensor="test"
        )
        df = pd.read_csv(join(tmp_path, "20200215_county_test.csv")).astype(
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
        assert_frame_equal(df, expected_df)

    def test_export_df_with_missingness(self, tmp_path):

        create_export_csv(
            df=self.DF2.copy(),
            export_dir=tmp_path,
            geo_res="county",
            sensor="test",
        )
        assert set(listdir(tmp_path)) == set(
            [
                "20200215_county_test.csv",
                "20200301_county_test.csv",
                "20200315_county_test.csv",
            ]
        )
        df = pd.read_csv(join(tmp_path, "20200215_county_test.csv")).astype(
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
        assert_frame_equal(df, expected_df)

    @mock.patch("delphi_utils.logger")
    def test_export_df_with_contradictory_missingness(self, mock_logger, tmp_path):

        create_export_csv(
            df=self.DF3.copy(),
            export_dir=tmp_path,
            geo_res="state",
            sensor="test",
            logger=mock_logger
        )
        assert set(listdir(tmp_path)) == set(
            [
                "20200215_state_test.csv",
                "20200301_state_test.csv",
                "20200315_state_test.csv",
            ]
        )
        assert pd.read_csv(join(tmp_path, "20200315_state_test.csv")).size > 0
        mock_logger.info.assert_called_once_with(
            "Filtering contradictory missing code in test_None_2020-02-15."
        )

    def test_export_sort(self, tmp_path):
        unsorted_df = pd.DataFrame({
            "geo_id": ["51175", "51093", "51175", "51620"],
            "timestamp": [
                datetime.strptime(x, "%Y-%m-%d")
                for x in ["2020-02-15", "2020-02-15", "2020-03-01", "2020-03-15"]
            ],
            "val": [3.12345678910, 2.1, 2.2, 2.6],
            "se": [0.15, 0.22, 0.20, 0.34],
            "sample_size": [100, 100, 101, 100],
        })
        create_export_csv(
            unsorted_df,
            export_dir=tmp_path,
            geo_res="county",
            sensor="test"
        )
        expected_df = pd.DataFrame({
            "geo_id": ["51175", "51093"],
            "val": [3.12345678910, 2.1],
            "se": [0.15, 0.22],
            "sample_size": [100, 100],
        })
        unsorted_csv = _set_df_dtypes(pd.read_csv(join(tmp_path, "20200215_county_test.csv")), dtypes={"geo_id": str})
        assert_frame_equal(unsorted_csv, expected_df)

        create_export_csv(
            unsorted_df,
            export_dir=tmp_path,
            geo_res="county",
            sensor="test",
            sort_geos=True
        )
        expected_df = pd.DataFrame({
            "geo_id": ["51093", "51175"],
            "val": [2.1, 3.12345678910],
            "se": [0.22, 0.15],
            "sample_size": [100, 100],
        })
        sorted_csv = _set_df_dtypes(pd.read_csv(join(tmp_path, "20200215_county_test.csv")), dtypes={"geo_id": str})
        assert_frame_equal(sorted_csv,expected_df)
