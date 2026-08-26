from os.path import join, exists

import pandas as pd
from datetime import datetime

from delphi_quidel_flutest.export import export_csv


class TestExport:
    def test_export(self):

        # create fake dataset and save in a temporary directory
        input_data = pd.DataFrame(
            {
                "geo_id": ["a", "a", "b", "b", "c", "c"],
                "val": [0, 2, 3, 5, 10, 12],
                "timestamp": [datetime(2020, 6, 2), datetime(2020, 6, 3)] * 3,
                "se": [0.01, 0.02, 0.01, 0.01, 0.005, 0.01],
                "sample_size": [100, 200, 500, 50, 80, 10]
            }
        )

        export_csv(
            input_data,
            geo_name="region",
            sensor="thing",
            receiving_dir="./receiving",
            start_date = datetime(2020, 6, 2),
            end_date = datetime(2020, 6, 3)
        )

        # check data for 2020-06-02
        expected_name = f"20200602_region_thing.csv"
        assert exists(join("./receiving", expected_name))

        output_data = pd.read_csv(join("./receiving", expected_name))

        assert (output_data.columns == ["geo_id", "val", "se", "sample_size"]).all()
        assert (output_data.geo_id == ["a", "b", "c"]).all()
        assert (output_data.se.values == [0.01, 0.01, 0.005]).all()
        assert (output_data.sample_size.values == [100, 500, 80]).all()

        # check data for 2020-06-03
        expected_name = f"20200603_region_thing.csv"
        assert exists(join("./receiving", expected_name))

        output_data = pd.read_csv(join("./receiving", expected_name))

        assert (output_data.columns == ["geo_id", "val", "se", "sample_size"]).all()
        assert (output_data.geo_id == ["a", "b", "c"]).all()
        assert (output_data.se.values == [0.02, 0.01, 0.01]).all()
        assert (output_data.sample_size.values == [200, 50, 10]).all()
