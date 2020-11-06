from os.path import join, exists
from tempfile import TemporaryDirectory
import pandas as pd
from pandas.testing import assert_frame_equal

from delphi_google_health.pull_api import (
    _api_data_to_df,
    _load_cached_file,
    _write_cached_file,
)

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

        #  create dummy dataset, write it, and load it back in
        df_input = pd.DataFrame({"bad": ["a"], "format": ["b"]})
        _write_cached_file(df_input, "FR", data_dir)
        df_output = _load_cached_file("FR", data_dir=data_dir)

        assert_frame_equal(df_input, df_output)

        #  make sure saved in the correct location
        assert exists(join(data_dir, "Data_FR_anosmia_ms.csv"))

        # remove temporary directory
        td.cleanup()
