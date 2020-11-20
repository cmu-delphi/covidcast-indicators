from os import listdir
from os.path import join

import pandas as pd

from delphi_safegraph_patterns.run import (run_module, METRICS,
                                           SENSORS, GEO_RESOLUTIONS)
                                         

class TestRun:
    def test_output_files(self, run_as_module):

        # Test output exists
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
