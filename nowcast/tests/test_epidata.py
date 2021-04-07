import csv
from datetime import date
import os
import tempfile
from unittest.mock import patch

import numpy as np
import pandas as pd

from delphi_nowcast.data_containers import LocationSeries, SensorConfig
from delphi_nowcast.epidata import export_to_csv

class TestExportToCSV:

    def test_export_to_csv(self):
        """Test export creates the right file and right contents."""
        test_sensor = SensorConfig(source="src",
                                   signal="sig",
                                   name="test",
                                   lag=4)
        test_value = LocationSeries("ca", "state", {date(2020, 1, 1): 1.5})
        with tempfile.TemporaryDirectory() as tmpdir:
            out_files = export_to_csv(test_value, test_sensor, date(2020, 1, 5), receiving_dir=tmpdir)
            assert len(out_files) == 1
            out_file = out_files[0]
            assert os.path.isfile(out_file)
            assert out_file.endswith("issue_20200105/src/20200101_state_sig.csv")
            out_file_df = pd.read_csv(out_file)
            pd.testing.assert_frame_equal(out_file_df,
                                          pd.DataFrame({"sensor_name": ["test"],
                                                        "geo_value": ["ca"],
                                                        "value": [1.5]}))
