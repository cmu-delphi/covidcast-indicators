from os import listdir
from os.path import join
from unittest.mock import patch

import pandas as pd

from delphi_safegraph_patterns.run import (run_module, METRICS,
                                           SENSORS, GEO_RESOLUTIONS)

class TestRun:
    PARAMS = {
        "common": {
            "export_dir": "./receiving"
        },
        "indicator": {
            "static_file_dir": "../static",
            "raw_data_dir": "./test_data/safegraph",
            "n_core": 12,
            "aws_access_key_id": "",
            "aws_secret_access_key": "",
            "aws_default_region": "",
            "aws_endpoint": "",
            "sync": False,
            "apikey":None,
        }
    }

    def mocked_construct_signals(*args, **kwargs):
        if args[1] == 'bars_visit_v2':
            return pd.read_csv('./test_data/df0.csv')
        if args[1] == 'restaurants_visit_v2':
            return pd.read_csv('./test_data/df1.csv')

    @patch("delphi_safegraph_patterns.process.data")
    @patch("delphi_safegraph_patterns.process.construct_signals", side_effect = mocked_construct_signals)
    def test_output_files(self, mock_construct_signals, mock_data):
        mock_data.return_value = None
        run_module(self.PARAMS)
        test_filepath = "./receiving"
        csv_files = listdir(test_filepath)

        dates = [
            "20210503", "20210504", "20210505", "20210506", "20210507",
            "20210508", "20210509",
        ]

        expected_files = []
        for date in dates:
            for geo in GEO_RESOLUTIONS:
                for sensor in SENSORS:
                    for metric in METRICS:
                        fn = "_".join([date, geo, metric[0], sensor]) + ".csv"
                        expected_files.append(fn)

        assert set(expected_files).issubset(set(csv_files))

        # Test output format
        df = pd.read_csv(
            join(test_filepath, "20210507_state_bars_visit_v2_num.csv")
        )
        assert (df.columns.values == ["geo_id", "val", "se", "sample_size"]).all()
