from datetime import datetime

import pandas as pd

from delphi_quidel_covidtest.geo_maps import geo_map, add_megacounties
from delphi_quidel_covidtest.constants import AGE_GROUPS

DATA_COLS = ['totalTest', 'numUniqueDevices', 'positiveTest']

class TestGeoMap:
    def test_county(self):

        df = pd.DataFrame(
            {
                "zip": [1607, 1740, 1001, 1003, 98661, 76010, 76012, 76016],
                "timestamp": ["2020-06-15", "2020-06-15", "2020-06-15", "2020-06-15",
                              "2020-06-15", "2020-06-15", "2020-06-15", "2020-06-15"],
                "totalTest_total": [10, 16, 10, 15, 20, 200, 250, 500],
                "positiveTest_total": [6, 8, 6, 8, 5, 5, 20, 50],
                "numUniqueDevices_total": [2, 1, 2, 1, 1, 1, 1, 1],
            }
        )
        
        for agegroup in AGE_GROUPS[1:]:
            df[f"totalTest_{agegroup}"] = [2, 3, 2, 2, 4, 40, 20, 25]
            df[f"positiveTest_{agegroup}"] = [1, 1, 1, 1, 1, 1, 3, 8]
            df[f"numUniqueDevices_{agegroup}"] = [2, 1, 2, 1, 1, 1, 1, 1]

        new_df, res_key = geo_map("county", df)

        assert res_key == 'fips'
        assert set(new_df["fips"].values) == set(['25027', '25013', '25015', '53011', '48439'])
        assert set(new_df["timestamp"].values) == set(df["timestamp"].values)
        assert set(new_df["totalTest_total"].values)  == set([26, 10, 15, 20, 950])
        assert set(new_df["positiveTest_total"].values) == set([14, 6, 8, 5, 75])

        assert set(new_df["totalTest_age_0_4"].values)  == set([5, 2, 2, 85, 4])
        assert set(new_df["positiveTest_age_0_4"].values) == set([2, 1, 1, 12, 1])
        
        # Test Megacounties
        new_df["timestamp"] = [datetime.strptime(x, "%Y-%m-%d") for x in new_df["timestamp"]]
        mega_df = add_megacounties(new_df, True)
        
        assert set(mega_df["totalTest_total"].values)  == set([26, 10, 15, 20, 950, 25, 20])
        assert set(mega_df["positiveTest_total"].values) == set([14, 6, 8, 5, 75, 14, 5])

        assert set(mega_df["totalTest_age_0_4"].values)  == set([5, 2, 2, 85, 4, 4, 9, 4])
        assert set(mega_df["positiveTest_age_0_4"].values) == set([2, 1, 1, 12, 1, 4, 1])
        

    def test_state(self):

        df = pd.DataFrame(
            {
                "zip": [1607, 1740, 98661, 76010, 76012, 76016],
                "timestamp": ["2020-06-15", "2020-06-15", "2020-06-15",
                              "2020-06-15", "2020-06-15", "2020-06-15"],
                "totalTest_total": [100, 50, 200, 200, 250, 500],
                "positiveTest_total": [10, 8, 15, 5, 20, 50],
                "numUniqueDevices_total": [2, 1, 1, 1, 1, 1]
            }
        )  
        for agegroup in AGE_GROUPS[1:]:
            df[f"totalTest_{agegroup}"] = [20, 10, 20, 40, 20, 25]
            df[f"positiveTest_{agegroup}"] = [2, 1, 3, 1, 3, 8]
            df[f"numUniqueDevices_{agegroup}"] = [2, 1, 1, 1, 1, 1]
        new_df, res_key = geo_map("state", df)


        assert set(new_df["state_id"].values) == set(['ma', 'tx', 'wa'])
        assert set(new_df["timestamp"].values) == set(df["timestamp"].values)
        assert set(new_df["totalTest_total"].values)  == set([150, 200, 950])
        assert set(new_df["positiveTest_total"].values) == set([18, 15, 75])
        
        assert set(new_df["totalTest_age_0_4"].values)  == set([30, 85, 20])
        assert set(new_df["positiveTest_age_0_4"].values) == set([3, 12, 3])

    def test_hrr(self):

        df = pd.DataFrame(
            {
                "zip": [1607, 98661, 76010, 76012, 74435, 74936],
                "timestamp": ["2020-06-15", "2020-06-15", "2020-06-15",
                              "2020-06-15", "2020-06-15", "2020-06-15"],
                "totalTest_total": [100, 50, 200, 200, 250, 500],
                "positiveTest_total": [10, 8, 15, 5, 20, 50],
                "numUniqueDevices_total": [2, 1, 1, 1, 1, 1]
            }
        )
        for agegroup in AGE_GROUPS[1:]:
            df[f"totalTest_{agegroup}"] = [20, 10, 20, 40, 20, 25]
            df[f"positiveTest_{agegroup}"] = [2, 1, 3, 1, 3, 8]
            df[f"numUniqueDevices_{agegroup}"] = [2, 1, 1, 1, 1, 1]

        new_df, _ = geo_map("hrr", df)

        assert set(new_df["hrr"].values) == set(["16", "231", "340", "344", "394"])
        assert set(new_df["timestamp"].values) == set(df["timestamp"].values)
        assert set(new_df["totalTest_total"].values)  == set([500, 100, 250, 50, 400])
        assert set(new_df["positiveTest_total"].values) == set([50, 10, 20, 8, 20])
        
        assert set(new_df["totalTest_age_0_4"].values)  == set([25, 20 ,20, 10, 60])
        assert set(new_df["positiveTest_age_0_4"].values) == set([8, 2, 3, 1, 4])

    def test_msa(self):

        df = pd.DataFrame(
            {
                "zip": [1607, 73716, 73719, 76010, 74945, 74936],
                "timestamp": ["2020-06-15", "2020-06-15", "2020-06-15",
                              "2020-06-15", "2020-06-15", "2020-06-15"],
                "totalTest_total": [100, 50, 200, 200, 250, 500],
                "positiveTest_total": [10, 8, 15, 5, 20, 50],
                "numUniqueDevices_total": [2, 1, 1, 1, 1, 1]
            }
        )
        for agegroup in AGE_GROUPS[1:]:
            df[f"totalTest_{agegroup}"] = [20, 10, 20, 40, 20, 25]
            df[f"positiveTest_{agegroup}"] = [2, 1, 3, 1, 3, 8]
            df[f"numUniqueDevices_{agegroup}"] = [2, 1, 1, 1, 1, 1]

        new_df, res_key = geo_map("msa", df)

        assert set(new_df["msa"].values) == set(['19100', '22900', '49340'])
        assert set(new_df["timestamp"].values) == set(df["timestamp"].values)
        assert set(new_df["totalTest_total"].values)  == set([200, 750, 100])
        assert set(new_df["positiveTest_total"].values) == set([5, 70, 10])
        
        assert set(new_df["totalTest_age_0_4"].values)  == set([40, 45, 20])
        assert set(new_df["positiveTest_age_0_4"].values) == set([1, 11, 2])

    def test_nation(self):
        df = pd.DataFrame(
            {
                "zip": [1607, 73716, 73719, 76010, 74945, 74936],
                "timestamp": ["2020-06-15", "2020-06-15", "2020-06-15",
                              "2020-06-15", "2020-06-15", "2020-06-15"],
                "totalTest_total": [100, 50, 200, 200, 250, 500],
                "positiveTest_total": [10, 8, 15, 5, 20, 50],
                "numUniqueDevices_total": [2, 1, 1, 1, 1, 1]
            }
        )
        for agegroup in AGE_GROUPS[1:]:
            df[f"totalTest_{agegroup}"] = [20, 10, 20, 40, 20, 25]
            df[f"positiveTest_{agegroup}"] = [2, 1, 3, 1, 3, 8]
            df[f"numUniqueDevices_{agegroup}"] = [2, 1, 1, 1, 1, 1]

        new_df, res_key = geo_map("nation", df)

        assert set(new_df["nation"].values) == set(["us"])
        assert set(new_df["timestamp"].values) == set(df["timestamp"].values)
        assert set(new_df["totalTest_total"].values) == set([1300])
        assert set(new_df["positiveTest_total"].values) == set([108])
        
        assert set(new_df["totalTest_age_0_4"].values)  == set([135])
        assert set(new_df["positiveTest_age_0_4"].values) == set([18])

    def test_hhs(self):
        df = pd.DataFrame(
            {
                "zip": [1607, 1740, 98661, 76010, 76012, 76016],
                "timestamp": ["2020-06-15", "2020-06-15", "2020-06-15",
                              "2020-06-15", "2020-06-15", "2020-06-15"],
                "totalTest_total": [100, 50, 200, 200, 250, 500],
                "positiveTest_total": [10, 8, 15, 5, 20, 50],
                "numUniqueDevices_total": [2, 1, 1, 1, 1, 1]
            }
        )
        for agegroup in AGE_GROUPS[1:]:
            df[f"totalTest_{agegroup}"] = [20, 10, 20, 40, 20, 25]
            df[f"positiveTest_{agegroup}"] = [2, 1, 3, 1, 3, 8]
            df[f"numUniqueDevices_{agegroup}"] = [2, 1, 1, 1, 1, 1]

        new_df, res_key = geo_map("hhs", df)

        assert set(new_df["hhs"].values) == set(["1", "6", "10"])
        assert set(new_df["timestamp"].values) == set(df["timestamp"].values)
        assert set(new_df["totalTest_total"].values)  == set([150, 200, 950])
        assert set(new_df["positiveTest_total"].values) == set([18, 15, 75])
        
        assert set(new_df["totalTest_age_0_4"].values)  == set([30, 20, 85])
        assert set(new_df["positiveTest_age_0_4"].values) == set([3, 3, 12])
