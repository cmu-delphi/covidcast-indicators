import pytest

from delphi_utils import read_params

from os.path import join, exists
from tempfile import TemporaryDirectory

import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal

from delphi_google_health.pull_api import (
    GoogleHealthTrends,
    get_counts_states,
    get_counts_dma,
    _get_counts_geoid,
    _api_data_to_df,
    _load_cached_file,
    _write_cached_file,
)


class TestGoogleHealthTrends:
    def test_class_state(self):

        params = read_params()
        ght = GoogleHealthTrends(ght_key=params["ght_key"])

        res = ght.query("2020-05-05", "2020-05-06", geo_id="AL", dma=False)
        assert [x for x in res.keys()] == ["lines"]
        assert [x["term"] for x in res["lines"]] == [
            "/m/0m7pl",
            "why cant i smell or taste",
            "loss of smell",
            "loss of taste",
        ]

        assert len(res["lines"][0]["points"]) == 2
        assert set([x for x in res["lines"][0]["points"][0].keys()]) == {"date", "value"}
        assert res["lines"][0]["points"][0]["date"] == "May 05 2020"
        assert res["lines"][0]["points"][1]["date"] == "May 06 2020"

    def test_class_dma(self):

        params = read_params()
        ght = GoogleHealthTrends(ght_key=params["ght_key"])

        res = ght.query("2020-05-05", "2020-05-06", geo_id=519, dma=True)
        assert [x for x in res.keys()] == ["lines"]
        assert [x["term"] for x in res["lines"]] == [
            "/m/0m7pl",
            "why cant i smell or taste",
            "loss of smell",
            "loss of taste",
        ]

        assert len(res["lines"][0]["points"]) == 2
        assert set([x for x in res["lines"][0]["points"][0].keys()]) == {"date", "value"}
        assert res["lines"][0]["points"][0]["date"] == "May 05 2020"
        assert res["lines"][0]["points"][1]["date"] == "May 06 2020"


class TestingPullCounts:
    def test_get_state_counts(self):

        params = read_params()
        ght = GoogleHealthTrends(ght_key=params["ght_key"])

        static_dir = join("..", "static")
        data_dir = join(".", "static_data")

        df_state = get_counts_states(
            ght, "2020-03-15", "2020-03-30", static_dir, data_dir
        )

        state_list = np.loadtxt(join(static_dir, "Canonical_STATE.txt"), dtype=str)

        assert (df_state.columns == ["geo_id", "timestamp", "val"]).all()
        assert df_state.shape == (len(state_list) * 16, 3)
        assert set(df_state["geo_id"].unique()) == set([x.lower() for x in state_list])

    def test_get_state_counts(self):

        params = read_params()
        ght = GoogleHealthTrends(ght_key=params["ght_key"])

        static_dir = join("..", "static")
        data_dir = join("..", "data")

        df_dma = get_counts_dma(ght, "2020-03-15", "2020-03-30", static_dir, data_dir)

        dma_list = np.loadtxt(join(static_dir, "Canonical_DMA.txt"), dtype=int)

        assert (df_dma.columns == ["geo_id", "timestamp", "val"]).all()
        assert df_dma.shape == (len(dma_list) * 16, 3)
        assert set(df_dma["geo_id"].unique()) == set(dma_list)


class TestGrabAPI:
    def test_api_grab(self):

        # Use a temporary cache directory to force pulling from the API
        params = read_params()
        ght = GoogleHealthTrends(ght_key=params["ght_key"])

        td = TemporaryDirectory()
        data_dir = td.name

        df = _get_counts_geoid(
            ght,
            geo_id="MA",
            start_date="2020-02-15",
            end_date="2020-02-17",
            dma=False,
            data_dir=data_dir,
        )

        assert df.shape == (3, 3)
        assert (df.columns == ["geo_id", "timestamp", "val"]).all()
        assert (df["geo_id"] == ["MA"] * 3).all()
        assert (df["timestamp"] == ["2020-02-15", "2020-02-16", "2020-02-17"]).all()

        # remove temporary directory
        td.cleanup()


class TestConvertJsonToDataFrame:
    def test_convert_process(self):

        dummy_data = {
            "lines": [
                {"term": "/m/0m7pl", "points": [{"date": "May 05 2020", "value": 1.2}]}
            ]
        }

        df = _api_data_to_df(dummy_data, "FR")
        assert df.shape == (1, 3)
        assert (df.columns == ["geo_id", "timestamp", "val"]).all()


class TestPullPushCache:
    def test_empty_cache(self):

        td = TemporaryDirectory()
        data_dir = td.name

        df = _load_cached_file("MA", data_dir=data_dir)

        assert df.shape == (0, 3)
        assert (df.columns == ["geo_id", "timestamp", "val"]).all()

        # remove temporary directory
        td.cleanup()

    def test_write_load(self):

        td = TemporaryDirectory()
        data_dir = td.name

        #  create dummy dataset, write it, and load it back in
        df_input = pd.DataFrame({"bad": ["a"], "format": ["b"]})
        _write_cached_file(df_input, "FR", data_dir)
        df_output = _load_cached_file("FR", data_dir=data_dir)

        assert_frame_equal(df_input, df_output)

        #  make sure saved in the correct location
        assert exists(join(data_dir, "Data_FR_anosmia_ms.csv"))

        # remove temporary directory
        td.cleanup()
