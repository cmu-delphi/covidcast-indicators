"""Tests for running the quidel module."""
from os import listdir
from os.path import join

import pandas as pd

from delphi_utils import add_prefix
from delphi_quidel.constants import GEO_RESOLUTIONS, SENSORS
from delphi_quidel.run import run_module

class TestRun:
    PARAMS = {
        "common": {
            "export_dir": "./receiving"
        },
        "indicator": {
            "static_file_dir": "../static",
            "input_cache_dir": "./cache",
            "export_start_date": {"covid_ag": "2020-06-30", "flu_ag": "2020-05-30"},
            "export_end_date": {"covid_ag": "2020-07-09", "flu_ag": "2020-07-05"},
            "pull_start_date": {"covid_ag": "2020-07-09","flu_ag": "2020-07-05"},
            "pull_end_date": {"covid_ag": "", "flu_ag": "2020-07-10"},
            "mail_server": "imap.exchange.andrew.cmu.edu",
            "account": "delphi-datadrop@andrew.cmu.edu",
            "password": "",
            "sender": "",
            "wip_signal": [""],
            "test_mode": True
        }
    }
    """Tests for running the module."""
    def test_output_files(self, clean_receiving_dir):
        """Tests that the output files contain the correct results of the run."""
        run_module(self.PARAMS)
        # Test output exists
        csv_files = listdir("receiving")

        dates_for_covid_ag = [
            "20200702",
            "20200703",
            "20200704",
            "20200705",
            "20200706",
            "20200707",
            "20200708",
            "20200709"
        ]

        dates_for_flu_ag = [
            "20200623",
            "20200624",
            "20200625",
            "20200626",
            "20200627",
            "20200628",
            "20200629",
            "20200630",
            "20200701",
            "20200702",
            "20200703"
        ]

        geos = GEO_RESOLUTIONS.copy()
        sensors = add_prefix(list(SENSORS.keys()),
                             wip_signal=self.PARAMS["indicator"]["wip_signal"],
                             prefix="wip_")

        expected_files = []
        for geo in geos:
            for sensor in sensors:
                if "covid_ag" in sensor:
                    for date in dates_for_covid_ag:
                        expected_files += [date + "_" + geo + "_" + sensor + ".csv"]
                else:
                    for date in dates_for_flu_ag:
                        expected_files += [date + "_" + geo + "_" + sensor + ".csv"]

        assert set(expected_files).issubset(set(csv_files))

        # Test output format
        df = pd.read_csv(
            join("./receiving", "20200709_state_covid_ag_raw_pct_positive.csv")
        )
        assert (df.columns.values == ["geo_id", "val", "se", "sample_size"]).all()

        # test_intermediate_file
        flag = None
        for fname in listdir("./cache"):
            if ".csv" in fname:
                flag = 1
        assert flag is not None
