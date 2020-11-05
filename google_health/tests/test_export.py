import pytest

from os.path import join, exists
from tempfile import TemporaryDirectory

import pandas as pd
import numpy as np

from delphi_google_health.export import export_csv, RESCALE_VAL


class TestGoogleHealthTrends:
    def test_export(self):

        # create fake dataset and save in a temporary directory
        input_data = pd.DataFrame(
            {
                "geo_id": ["a", "a", "b", "b", "c", "c"],
                "val": [0, 2, 3, 5, 10, 12],
                "timestamp": ["2020-02-02", "2020-02-03"] * 3,
            }
        )
        
        start_date = "2020-02-02"
        
        td = TemporaryDirectory()
        export_csv(
            input_data,
            geo_name="region",
            sensor="thing",
            smooth=False,
            start_date=start_date,
            receiving_dir=td.name,
        )

        # check data for 2020-02-02
        expected_name = f"20200202_region_thing.csv"
        assert exists(join(td.name, expected_name))

        output_data = pd.read_csv(join(td.name, expected_name))

        assert (output_data.columns == ["geo_id", "val", "se", "sample_size"]).all()
        assert (output_data.geo_id == ["a", "b", "c"]).all()
        assert (output_data.val == (np.array([0, 3, 10]) / RESCALE_VAL)).all()
        assert np.isnan(output_data.se.values).all()
        assert np.isnan(output_data.sample_size.values).all()

        # check data for 2020-02-03
        expected_name = f"20200203_region_thing.csv"
        assert exists(join(td.name, expected_name))

        output_data = pd.read_csv(join(td.name, expected_name))

        assert (output_data.columns == ["geo_id", "val", "se", "sample_size"]).all()
        assert (output_data.geo_id == ["a", "b", "c"]).all()
        assert (output_data.val == np.array([2, 5, 12]) / RESCALE_VAL).all()
        assert np.isnan(output_data.se.values).all()
        assert np.isnan(output_data.sample_size.values).all()

        # remove temporary directory
        td.cleanup()
