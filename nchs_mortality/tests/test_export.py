from datetime import datetime
from os.path import join, exists

import pandas as pd

from delphi_nchs_mortality.export import export_csv
from delphi_utils import Nans


class TestExport:
    def test_export(self):

        # create fake dataset and save in a temporary directory
        input_data = pd.DataFrame(
            {
                "geo_id": ["a", "a", "b", "b", "c", "c"],
                "val": [0, 2, 3, 5, 10, 12],
                "timestamp": [datetime(2020, 6, 2), datetime(2020, 6, 9)] * 3,
                "se": [0.01, 0.02, 0.01, 0.01, 0.005, 0.01],
                "sample_size": [100, 200, 500, 50, 80, 10],
                "missing_val": [Nans.NOT_MISSING] * 6,
                "missing_se": [Nans.NOT_MISSING] * 6,
                "missing_sample_size": [Nans.NOT_MISSING] * 6,
            }
        )

        export_csv(
            input_data,
            geo_name = "state",
            sensor="region_thing",
            export_dir="./receiving",
            start_date = datetime(2020, 6, 2),
        )

        # check data for 2020-06-02
        expected_name = "weekly_202023_state_region_thing.csv"
        assert exists(join("./receiving", expected_name))

        output_data = pd.read_csv(join("./receiving", expected_name))

        expected_columns = [
            "geo_id", "val", "se", "sample_size",
            "missing_val", "missing_se", "missing_sample_size"
        ]
        assert (output_data.columns == expected_columns).all()
        assert (output_data.geo_id == ["a", "b", "c"]).all()
        assert (output_data.se.values == [0.01, 0.01, 0.005]).all()
        assert (output_data.sample_size.values == [100, 500, 80]).all()

        # check data for 2020-06-03
        expected_name = "weekly_202024_state_region_thing.csv"
        assert exists(join("./receiving", expected_name))

        output_data = pd.read_csv(join("./receiving", expected_name))

        assert (output_data.columns == expected_columns).all()
        assert (output_data.geo_id == ["a", "b", "c"]).all()
        assert (output_data.se.values == [0.02, 0.01, 0.01]).all()
        assert (output_data.sample_size.values == [200, 50, 10]).all()
