import pandas as pd

from delphi_quidel_covidtest.geo_maps import geo_map


class TestGeoMap:
    def test_county(self):

        df = pd.DataFrame(
            {
                "zip": [1607, 1740, 98661, 76010, 76012, 76016],
                "timestamp": ["2020-06-15", "2020-06-15", "2020-06-15",
                              "2020-06-15", "2020-06-15", "2020-06-15"],
                "totalTest": [100, 50, 200, 200, 250, 500],
                "positiveTest": [10, 8, 15, 5, 20, 50],
                "numUniqueDevices": [2, 1, 1, 1, 1, 1]
            }
        )

        new_df, res_key = geo_map("county", df)

        assert res_key == 'fips'
        assert set(new_df["fips"].values) == set(['25027', '53011', '48439'])
        assert set(new_df["timestamp"].values) == set(df["timestamp"].values)
        assert set(new_df["totalTest"].values)  == set([150, 200, 950])
        assert set(new_df["positiveTest"].values) == set([18, 15, 75])

    def test_state(self):

        df = pd.DataFrame(
            {
                "zip": [1607, 1740, 98661, 76010, 76012, 76016],
                "timestamp": ["2020-06-15", "2020-06-15", "2020-06-15",
                              "2020-06-15", "2020-06-15", "2020-06-15"],
                "totalTest": [100, 50, 200, 200, 250, 500],
                "positiveTest": [10, 8, 15, 5, 20, 50],
                "numUniqueDevices": [2, 1, 1, 1, 1, 1]
            }
        )

        new_df = geo_map("state", df)

        assert set(new_df["state_id"].values) == set(['ma', 'tx', 'wa'])
        assert set(new_df["timestamp"].values) == set(df["timestamp"].values)
        assert set(new_df["totalTest"].values)  == set([150, 200, 950])
        assert set(new_df["positiveTest"].values) == set([18, 15, 75])

    def test_hrr(self):

        df = pd.DataFrame(
            {
                "zip": [1607, 98661, 76010, 76012, 74435, 74936],
                "timestamp": ["2020-06-15", "2020-06-15", "2020-06-15",
                              "2020-06-15", "2020-06-15", "2020-06-15"],
                "totalTest": [100, 50, 200, 200, 250, 500],
                "positiveTest": [10, 8, 15, 5, 20, 50],
                "numUniqueDevices": [2, 1, 1, 1, 1, 1]
            }
        )

        new_df, _ = geo_map("hrr", df)

        assert set(new_df["hrr"].values) == set(["16", "231", "340", "344", "394"])
        assert set(new_df["timestamp"].values) == set(df["timestamp"].values)
        assert set(new_df["totalTest"].values)  == set([500, 100, 250, 50, 400])
        assert set(new_df["positiveTest"].values) == set([50, 10, 20, 8, 20])

    def test_msa(self):

        df = pd.DataFrame(
            {
                "zip": [1607, 73716, 73719, 76010, 74945, 74936],
                "timestamp": ["2020-06-15", "2020-06-15", "2020-06-15",
                              "2020-06-15", "2020-06-15", "2020-06-15"],
                "totalTest": [100, 50, 200, 200, 250, 500],
                "positiveTest": [10, 8, 15, 5, 20, 50],
                "numUniqueDevices": [2, 1, 1, 1, 1, 1]
            }
        )

        new_df, res_key = geo_map("msa", df)

        assert res_key == 'msa'
        assert set(new_df["msa"].values) == set(['19100', '22900', '49340'])
        assert set(new_df["timestamp"].values) == set(df["timestamp"].values)
        assert set(new_df["totalTest"].values)  == set([200, 750, 100])
        assert set(new_df["positiveTest"].values) == set([5, 70, 10])
