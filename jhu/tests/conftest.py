# -*- coding: utf-8 -*-

from os import listdir, remove
from os.path import join

import pandas as pd
import pytest

from delphi_jhu.run import run_module

PARAMS =  {
    "common": {
        "export_dir": "./receiving"
    },
    "indicator": {
        "base_url": "test_data/small_{metric}.csv",
        "export_start_date": "2020-03-03",
        "static_file_dir": "../static"
    }
}

@pytest.fixture(scope="session")
def run_as_module():
    # Clean receiving directory
    for fname in listdir("receiving"):
        if fname != ".gitkeep":
            remove(join("receiving", fname))

    run_module(PARAMS)

@pytest.fixture
def jhu_confirmed_test_data():
    df = pd.read_csv("test_data/jhu_confirmed.csv", dtype={"fips": str})
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df
