"""Tests for delphi_nssp_ed.run (end-to-end with the static feed fixture)."""

from pathlib import Path

import pandas as pd
import pytest

from delphi_nssp_ed.constants import GEOS, SIGNALS, SIGNALS_BASE, SIGNALS_SMOOTHED, SMOOTHED_SUFFIX
from delphi_nssp_ed.run import add_needed_columns, add_smoothed_signals, map_state_geocode, run_module

from conftest import TEST_DATA, cleanup

TEST_DIR = Path(__file__).parent


class TestHelpers:
    def test_add_needed_columns(self):
        df = pd.DataFrame({"geo_id": ["us"], "val": [1.0]})
        df = add_needed_columns(df, col_names=None)
        assert df.columns.tolist() == [
            "geo_id",
            "val",
            "se",
            "sample_size",
            "missing_val",
            "missing_se",
            "missing_sample_size",
        ]
        assert df["se"].isnull().all()
        assert df["sample_size"].isnull().all()

    def test_map_state_geocode(self):
        from delphi_utils import get_structured_logger

        assert map_state_geocode("California") == "ca"
        assert map_state_geocode("District of Columbia") == "dc"
        logger = get_structured_logger("test_map_state_geocode")
        assert map_state_geocode("Not A State", logger=logger) is None

    def test_add_smoothed_signals(self, pulled_df):
        df = add_smoothed_signals(pulled_df)
        for signal in SIGNALS_SMOOTHED:
            assert signal in df.columns
        # the first 6 days of each geo series need a full 7-day window
        alabama = df[df["geography"] == "Alabama"].sort_values("timestamp")
        assert alabama[SIGNALS_SMOOTHED[0]].iloc[:6].isna().all()
        assert alabama[SIGNALS_SMOOTHED[0]].iloc[6:].notna().all()
        # spot-check the trailing mean math
        base = SIGNALS_BASE[0]
        expected = alabama[base].iloc[0:7].mean()
        assert alabama[SIGNALS_SMOOTHED[0]].iloc[6] == pytest.approx(expected)


class TestRunModule:
    def test_output_files(self, params, socrata_mock):
        run_module(params)
        export_dir = Path(params["common"]["export_dir"])
        csv_files = [f.name for f in export_dir.glob("*.csv")]

        # 15 fixture days; smoothed signals lose the first 6 days each
        raw_dates = [f"202609{d:02d}" for d in range(5, 20)]
        smoothed_dates = [f"202609{d:02d}" for d in range(11, 20)]
        expected = set()
        for geo in GEOS:
            for signal in SIGNALS_BASE:
                expected.update(f"{d}_{geo}_{signal}.csv" for d in raw_dates)
            for signal in SIGNALS_SMOOTHED:
                expected.update(f"{d}_{geo}_{signal}.csv" for d in smoothed_dates)
        assert set(csv_files) == expected

        # every file has the covidcast export columns and no null vals
        sample = pd.read_csv(export_dir / f"20260919_state_{SIGNALS_BASE[0]}.csv")
        assert {"geo_id", "val", "se", "sample_size"}.issubset(sample.columns)
        assert sample["val"].notna().all()
        # se/sample_size are always missing for this feed
        assert sample["se"].isna().all()
        assert sample["sample_size"].isna().all()

        cleanup(params)

    def test_geo_ids(self, params, socrata_mock):
        run_module(params)
        export_dir = Path(params["common"]["export_dir"])

        nation = pd.read_csv(export_dir / f"20260919_nation_{SIGNALS_BASE[0]}.csv")
        assert set(nation["geo_id"]) == {"us"}

        state = pd.read_csv(export_dir / f"20260919_state_{SIGNALS_BASE[0]}.csv")
        assert "ca" in set(state["geo_id"])
        assert "dc" in set(state["geo_id"])
        assert "us" not in set(state["geo_id"])
        assert len(state) == 51  # 50 states + DC

        hhs = pd.read_csv(export_dir / f"20260919_hhs_{SIGNALS_BASE[0]}.csv")
        assert set(hhs["geo_id"].astype(str)) == {str(i) for i in range(1, 11)}
        # HHS values are population-weighted means of state percentages
        assert hhs["val"].between(0, 100).all()

        cleanup(params)

    def test_empty_feed_exports_nothing(self, params):
        from unittest.mock import patch as mock_patch

        with mock_patch("delphi_nssp_ed.pull.Socrata") as mock_cls:
            mock_cls.return_value.get.side_effect = [[]]
            with pytest.raises(ValueError, match="schema may"):
                run_module(params)
        cleanup(params)

    def test_export_start_date_filters_output(self, params, socrata_mock):
        params["indicator"]["export_start_date"] = "2026-09-15"
        run_module(params)
        export_dir = Path(params["common"]["export_dir"])
        csv_files = [f.name for f in export_dir.glob("*.csv")]
        dates = {f.split("_")[0] for f in csv_files}
        assert min(dates) >= "20260915"
        cleanup(params)
