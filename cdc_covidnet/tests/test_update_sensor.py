from datetime import datetime
import json
from os.path import join, exists
from tempfile import TemporaryDirectory

import numpy as np
import pandas as pd

from delphi_cdc_covidnet.update_sensor import update_sensor


class TestUpdateSensor:

    def test_syn_update_sensor(self):
        with TemporaryDirectory() as temp_dir:
            # Create synthetic data
            state_1 = {"datadownload": [
                {
                    "catchment": "California", "network": "Network A", "age_category": "Overall",
                    "year": "2020", "mmwr-year": "2020", "mmwr-week": "10",
                    "cumulative-rate": 2.5, "weekly-rate": 0.7
                }, {
                    "catchment": "California", "network": "Network A", "age_category": "Overall",
                    "year": "2020", "mmwr-year": "2020", "mmwr-week": "11",
                    "cumulative-rate": 3.5, "weekly-rate": 1.4
                }, {
                    "catchment": "California", "network": "Network A", "age_category": "Overall",
                    "year": "2020", "mmwr-year": "2020", "mmwr-week": "12",
                    "cumulative-rate": 4.2, "weekly-rate": 1.9
                }]}

            state_2 = {"datadownload": [
                {
                    "catchment": "Pennsylvania", "network": "Network B", "age_category": "Overall",
                    "year": "2020", "mmwr-year": "2020", "mmwr-week": "10",
                    "cumulative-rate": 10.3, "weekly-rate": 0.9
                }, {
                    "catchment": "Pennsylvania", "network": "Network B", "age_category": "Overall",
                    "year": "2020", "mmwr-year": "2020", "mmwr-week": "11",
                    "cumulative-rate": 11.2, "weekly-rate": 4.5
                }, {
                    "catchment": "Pennsylvania", "network": "Network B", "age_category": "Overall",
                    "year": "2020", "mmwr-year": "2020", "mmwr-week": "12",
                    "cumulative-rate": 11.8, "weekly-rate": 1.2
                }]}

            state_files = [join(temp_dir, state) for state in ["state_1.json", "state_2.json"]]
            with open(state_files[0], "w") as f_json:
                json.dump(state_1, f_json)
            with open(state_files[1], "w") as f_json:
                json.dump(state_2, f_json)

            for state_file in state_files:
                assert exists(state_file)

            mmwr_info = pd.DataFrame([
                {
                    "mmwrid": 3036, "weekend": "2020-03-07", "weeknumber": 10,
                    "weekstart": "2020-03-01", "year": 2020, "seasonid": 59
                }, {
                    "mmwrid": 3037, "weekend": "2020-03-14", "weeknumber": 11,
                    "weekstart": "2020-03-08", "year": 2020, "seasonid": 59
                }, {
                    "mmwrid": 3038, "weekend": "2020-03-21", "weeknumber": 12,
                    "weekstart": "2020-03-15", "year": 2020, "seasonid": 59
                }])
            mmwr_info["weekstart"] = pd.to_datetime(mmwr_info["weekstart"])
            mmwr_info["weekend"] = pd.to_datetime(mmwr_info["weekend"])

            # End date set up to be before last week of data
            start_date = datetime(year=2020, month=3, day=7)
            end_date = datetime(year=2020, month=3, day=17)

            # Generate the csvs
            hosp_df = update_sensor(state_files, mmwr_info, temp_dir, start_date, end_date, "")
            # Check dataframe returned
            assert hosp_df.index.nlevels == 2
            assert set(hosp_df.index.names) == {"date", "geo_id"}
            assert set(hosp_df.index.get_level_values("geo_id")) == {"ca", "pa"}
            assert set(hosp_df.index.get_level_values("date")) == \
                    {datetime(2020, 3, 7), datetime(2020, 3, 14)}
            assert set(hosp_df["epiweek"].unique()) == {10, 11}
            geo_index = hosp_df.index.get_level_values("geo_id")
            assert np.allclose(hosp_df.loc[geo_index == "ca", "val"], [2.5, 3.5])
            assert np.allclose(hosp_df.loc[geo_index == "pa", "val"], [10.3, 11.2])
            assert pd.isna(hosp_df["se"]).all()
            assert pd.isna(hosp_df["sample_size"]).all()

            # Check actual files generated
            expected_files = ["202010_state_covidnet.csv", "202011_state_covidnet.csv"]
            expected_files = [join(temp_dir, exp_file) for exp_file in expected_files]
            for exp_file in expected_files:
                assert exists(exp_file)
            assert not exists("202012_state_covidnet.csv")

            for i, exp_file in enumerate(expected_files):
                data = pd.read_csv(exp_file)
                assert (data.columns == ["geo_id", "val", "se", "sample_size"]).all()

                # Check data for NA
                assert (~pd.isna(data["geo_id"])).all()
                assert (~pd.isna(data["val"])).all()
                assert pd.isna(data["se"]).all()
                assert pd.isna(data["sample_size"]).all()

                # Check values are right
                assert set(data["geo_id"].unique()) == {"ca", "pa"}
                assert np.allclose(
                    data["val"], [
                        state_1["datadownload"][i]["cumulative-rate"],
                        state_2["datadownload"][i]["cumulative-rate"]])
