from os import listdir
from os.path import join
from unittest.mock import patch

import pandas as pd

from delphi_safegraph_patterns.run import (run_module, METRICS,
                                           SENSORS, GEO_RESOLUTIONS)

OLD_VERSIONS = [
            # release version, access dir
            ("202004", "weekly-patterns/v2", "main-file/*.csv.gz"),
            ("202006", "weekly-patterns-delivery/weekly", "patterns/*/*/*"),
            ("20210408", "weekly-patterns-delivery-2020-12/weekly", "patterns/*/*/*")
    ]

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

    @patch("delphi_safegraph_patterns.run.VERSIONS", OLD_VERSIONS)
    def test_output_files(self, clean_receiving_dir):
        run_module(self.PARAMS)
        csv_files = listdir("receiving")

        dates = [
            "20190722", "20190723", "20190724", "20190725", "20190726",
            "20190727", "20190728", "20190729", "20190730", "20190731",
            "20190801", "20190802", "20190803", "20190804",
            "20200727", "20200728", "20200729", "20200730", "20200731",
            "20200801", "20200802", "20200803", "20200804", "20200805",
            "20200806", "20200807", "20200808", "20200809"
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
            join("./receiving", "20200729_state_bars_visit_num.csv")
        )
        assert (df.columns.values == ["geo_id", "val", "se", "sample_size"]).all()
