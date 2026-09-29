"""Unit tests for delphi_nssp_ed.pull."""

import json
from pathlib import Path
from unittest.mock import patch as mock_patch

import pandas as pd
import pytest

from delphi_nssp_ed.constants import EXPECTED_COLUMNS, SIGNALS_BASE, SIGNALS_MAP
from delphi_nssp_ed.pull import (
    drop_implausible_values,
    pivot_signals,
    pull_nssp_ed_data,
    pull_with_socrata_api,
    validate_schema,
)

TEST_DIR = Path(__file__).parent

with open(f"{TEST_DIR}/test_data/page.json", "r") as f:
    TEST_DATA = json.load(f)


def make_long_df(rows):
    return pd.DataFrame.from_records(rows)


class TestValidateSchema:
    def test_ok(self):
        df = make_long_df(TEST_DATA)
        validate_schema(df)  # should not raise

    def test_missing_column_raises(self):
        df = make_long_df(TEST_DATA).drop(columns=["percent_visits"])
        with pytest.raises(ValueError, match="schema may"):
            validate_schema(df)


class TestDropImplausibleValues:
    def test_drops_out_of_range_and_non_numeric(self):
        rows = [
            {"date": "2026-09-19T00:00:00.000", "pathogen": "COVID", "geography": "Alabama", "percent_visits": "1.5"},
            {"date": "2026-09-19T00:00:00.000", "pathogen": "COVID", "geography": "Alaska", "percent_visits": "150"},
            {"date": "2026-09-19T00:00:00.000", "pathogen": "COVID", "geography": "Arizona", "percent_visits": "-2"},
            {"date": "2026-09-19T00:00:00.000", "pathogen": "COVID", "geography": "Arkansas", "percent_visits": "N/A"},
            {"date": "2026-09-19T00:00:00.000", "pathogen": "COVID", "geography": "California", "percent_visits": "100"},
            {"date": "2026-09-19T00:00:00.000", "pathogen": "COVID", "geography": "Colorado", "percent_visits": "0"},
        ]
        df = drop_implausible_values(make_long_df(rows))
        assert set(df["geography"]) == {"Alabama", "California", "Colorado"}
        assert df["percent_visits"].between(0, 100).all()

    def test_boundary_values_kept(self):
        rows = [
            {"date": "2026-09-19T00:00:00.000", "pathogen": "RSV", "geography": "Alabama", "percent_visits": "0"},
            {"date": "2026-09-19T00:00:00.000", "pathogen": "RSV", "geography": "Alaska", "percent_visits": "100"},
        ]
        df = drop_implausible_values(make_long_df(rows))
        assert len(df) == 2


class TestPivotSignals:
    def test_pivot_columns_and_values(self):
        df = make_long_df(TEST_DATA)
        df = df.rename(columns={"date": "timestamp"}).astype(
            {"timestamp": "datetime64[ns]", "geography": str, "pathogen": str}
        )
        df["percent_visits"] = pd.to_numeric(df["percent_visits"])
        pivoted = pivot_signals(df)
        assert list(pivoted.columns) == ["timestamp", "geography"] + SIGNALS_BASE
        # spot-check one value against the raw fixture
        raw = [r for r in TEST_DATA if r["geography"] == "Alabama" and r["date"] == "2026-09-19T00:00:00.000"]
        row = pivoted[
            (pivoted["geography"] == "Alabama") & (pivoted["timestamp"] == "2026-09-19")
        ].iloc[0]
        for r in raw:
            assert row[SIGNALS_MAP[r["pathogen"]]] == pytest.approx(float(r["percent_visits"]))

    def test_unknown_pathogen_raises(self):
        df = make_long_df(TEST_DATA)
        df.loc[df.index[0], "pathogen"] = "Norovirus"
        df = df.rename(columns={"date": "timestamp"})
        with pytest.raises(ValueError, match="Unknown pathogen"):
            pivot_signals(df)

    def test_missing_pathogen_column_is_nan(self):
        df = make_long_df([r for r in TEST_DATA if r["pathogen"] != "RSV"])
        df = df.rename(columns={"date": "timestamp"}).astype(
            {"timestamp": "datetime64[ns]", "geography": str, "pathogen": str}
        )
        df["percent_visits"] = pd.to_numeric(df["percent_visits"])
        pivoted = pivot_signals(df)
        assert SIGNALS_MAP["RSV"] in pivoted.columns
        assert pivoted[SIGNALS_MAP["RSV"]].isna().all()


class TestPullWithSocrataApi:
    def test_paginates_until_empty(self):
        page1 = [{"a": 1}] * 3
        with mock_patch("delphi_nssp_ed.pull.Socrata") as mock_cls:
            mock_cls.return_value.get.side_effect = [page1, []]
            results = pull_with_socrata_api("some-id", socrata_token=None)
        assert results == page1
        # anonymous access: token None is passed through to Socrata
        assert mock_cls.call_args[0][1] is None

    def test_token_passed_through(self):
        with mock_patch("delphi_nssp_ed.pull.Socrata") as mock_cls:
            mock_cls.return_value.get.side_effect = [[]]
            pull_with_socrata_api("some-id", socrata_token="tok123")
        assert mock_cls.call_args[0][1] == "tok123"


class TestPullNsspEdData:
    def test_live_pull_path_conforms_fixture(self, tmp_path):
        with mock_patch("delphi_nssp_ed.pull.Socrata") as mock_cls:
            mock_cls.return_value.get.side_effect = [TEST_DATA, []]
            df = pull_nssp_ed_data(str(tmp_path), custom_run=False, socrata_token=None)
        assert list(df.columns) == ["timestamp", "geography"] + SIGNALS_BASE
        assert len(df) == 15 * 52  # 15 days x 52 geographies
        # raw backup was written
        assert list(tmp_path.glob("*.csv.gz"))

    def test_custom_run_requires_source_df(self, tmp_path):
        with pytest.raises(ValueError, match="source_df is required"):
            pull_nssp_ed_data(str(tmp_path), custom_run=True, source_df=None)

    def test_custom_run_skips_backup(self, tmp_path):
        df = pull_nssp_ed_data(
            str(tmp_path),
            custom_run=True,
            source_df=make_long_df(TEST_DATA),
        )
        assert list(df.columns) == ["timestamp", "geography"] + SIGNALS_BASE
        assert list(tmp_path.glob("*")) == []

    def test_empty_feed_raises_schema_error(self, tmp_path):
        with mock_patch("delphi_nssp_ed.pull.Socrata") as mock_cls:
            mock_cls.return_value.get.side_effect = [[]]
            with pytest.raises(ValueError, match="schema may"):
                pull_nssp_ed_data(str(tmp_path), custom_run=False)
