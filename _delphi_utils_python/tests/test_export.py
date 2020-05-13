import pytest

from datetime import datetime
from os import listdir, remove
from os.path import join

import pandas as pd
from delphi_utils import create_export_csv


class TestExport:
    def test_export_csv(self):

        # Clean receiving directory
        for fname in listdir("test_dir"):
            remove(join("test_dir", fname))

        times = [
            datetime.strptime(x, "%Y-%m-%d")
            for x in ["2020-02-15", "2020-02-15", "2020-03-01", "2020-03-15"]
        ]
        df = pd.DataFrame(
            {
                "geo_id": ["51093", "51175", "51175", "51620"],
                "timestamp": times,
                "val": [3.6, 2.1, 2.2, 2.6],
                "se": [0.15, 0.22, 0.20, 0.34],
                "sample_size": [100, 100, 101, 100],
            }
        )

        create_export_csv(
            df=df,
            start_date=datetime.strptime("2020-02-15", "%Y-%m-%d"),
            export_dir="test_dir",
            metric="deaths",
            geo_res="county",
            sensor="test",
        )

        assert set(listdir("test_dir")) == set(
            [
                "20200215_county_deaths_test.csv",
                "20200301_county_deaths_test.csv",
                "20200315_county_deaths_test.csv",
            ]
        )
