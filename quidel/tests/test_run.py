from os import listdir, remove
from os.path import join

import pandas as pd

from delphi_utils import read_params, add_prefix
from delphi_quidel.run import run_module
from delphi_quidel.constants import GEO_RESOLUTIONS, SENSORS


class TestRun:
    def test_output_files(self, run_as_module):
        
        params = read_params()
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
                             wip_signal=params["wip_signal"],
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
