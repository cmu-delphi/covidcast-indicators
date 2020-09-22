from delphi_utils.geomap import GeoMapper

import pytest

import pandas as pd
import numpy as np


class TestGeoMapper:
    fips_data = pd.DataFrame(
        {
            "fips": ["01123", "02340", "98633", "18181"],
            "date": [pd.Timestamp("2018-01-01")] * 4,
            "count": [2, 0, 20, 10021],
            "total": [4, 0, 400, 100001],
        }
    )
    fips_data_2 = pd.DataFrame(
        {
            "fips": ["01123", "02340", "02002", "18633", "18181"],
            "date": [pd.Timestamp("2018-01-01")] * 5,
            "count": [2, 1, 20, np.nan, 10021],
            "total": [4, 1, 400, np.nan, 100001],
        }
    )
    fips_data_3 = pd.DataFrame(
        {
            "fips": ["48059", "48253", "48441", "72003", "72005", "10999"],
            "date": [pd.Timestamp("2018-01-01")] * 3 + [pd.Timestamp("2018-01-03")] * 3,
            "count": [1, 2, 3, 4, 8, 5],
            "total": [2, 4, 7, 11, 100, 10],
        }
    )
    fips_data_4 = pd.DataFrame(
        {
            "fips": ["01123", "48253", "72003", "18181"],
            "date": [pd.Timestamp("2018-01-01")] * 4,
            "count": [2, 1, np.nan, 10021],
            "total": [4, 1, np.nan, 100001],
        }
    )
    zip_data = pd.DataFrame(
        {
            "zip": ["45140", "95616", "95618"] * 2,
            "date": [pd.Timestamp("2018-01-01")] * 3 + [pd.Timestamp("2018-01-03")] * 3,
            "count": [99, 345, 456, 100, 344, 442],
        }
    )
    zip_data["total"] = zip_data["count"] * 2
    jan_month = pd.bdate_range("2018-01-01", "2018-02-01")
    mega_data = pd.concat(
        (
            pd.DataFrame(
                {
                    "fips": ["01001"] * len(jan_month),
                    "date": jan_month,
                    "count": np.arange(len(jan_month)),
                    "visits": np.arange(len(jan_month)),
                }
            ),
            pd.DataFrame(
                {
                    "fips": ["01002"] * len(jan_month),
                    "date": jan_month,
                    "count": np.arange(len(jan_month)),
                    "visits": 2 * np.arange(len(jan_month)),
                }
            ),
        )
    )
    jhu_data = pd.DataFrame(
        {
            "fips_jhu": ["48059", "48253", "72005", "10999", "90010", 70002],
            "date": [pd.Timestamp("2018-01-01")] * 3 + [pd.Timestamp("2018-01-03")] * 3,
            "count": [1, 2, 3, 4, 8, 5],
            "total": [2, 4, 7, 11, 100, 10],
        }
    )
    jhu_uid_data = pd.DataFrame(
        {
            "jhu_uid": [
                84048315,
                84048137,
                84013299,
                84013299,
                84070002,
                84000013,
                84090002,
            ],
            "date": [pd.Timestamp("2018-01-01")] * 3
            + [pd.Timestamp("2018-01-03")] * 3
            + [pd.Timestamp("2018-01-01")],
            "count": [1, 2, 3, 4, 8, 5, 20],
            "total": [2, 4, 7, 11, 100, 10, 40],
        }
    )
    # jhu_big_data = pd.read_csv("test_dir/small_deaths.csv")

    # Loading tests updated 8/26
    def test_load_zip_fips_table(self):
        gmpr = GeoMapper()
        fips_data = gmpr._load_crosswalk(from_code="zip", to_code="fips")
        assert (fips_data.groupby("zip").sum()["weight"] == 0).sum() == 144
        assert set(fips_data.columns) == set(["zip", "fips", "weight"])
        assert pd.api.types.is_string_dtype(fips_data.zip)
        assert pd.api.types.is_string_dtype(fips_data.fips)
        assert pd.api.types.is_float_dtype(fips_data.weight)

    def test_load_state_table(self):
        gmpr = GeoMapper()
        state_data = gmpr._load_crosswalk(from_code="state", to_code="state")
        assert tuple(state_data.columns) == ("state_code", "state_id", "state_name")
        assert state_data.shape[0] == 60

    def test_load_fips_msa_table(self):
        gmpr = GeoMapper()
        msa_data = gmpr._load_crosswalk(from_code="fips", to_code="msa")
        assert tuple(msa_data.columns) == ("fips", "msa")

    def test_load_jhu_uid_fips_table(self):
        gmpr = GeoMapper()
        jhu_data = gmpr._load_crosswalk(from_code="jhu_uid", to_code="fips")
        assert (jhu_data.groupby("jhu_uid").sum() == 1).all()[0]

    def test_load_zip_hrr_table(self):
        gmpr = GeoMapper()
        zip_data = gmpr._load_crosswalk(from_code="zip", to_code="hrr")
        assert pd.api.types.is_string_dtype(zip_data["zip"])
        assert pd.api.types.is_string_dtype(zip_data["hrr"])

    def test_convert_fips_to_state_code(self):
        gmpr = GeoMapper()
        new_data = gmpr.convert_fips_to_state_code(self.fips_data)
        assert new_data["state_code"].dtype == "O"
        assert new_data.loc[1, "state_code"] == new_data.loc[1, "fips"][:2]

    def test_fips_to_state_code(self):
        gmpr = GeoMapper()
        new_data = gmpr.fips_to_state_code(self.fips_data_3)
        assert np.allclose(new_data["count"].sum(), self.fips_data_3["count"].sum())

    def test_convert_state_code_to_state_id(self):
        gmpr = GeoMapper()
        new_data = gmpr.convert_fips_to_state_code(self.fips_data)
        new_data = gmpr.convert_state_code_to_state_id(new_data)
        assert new_data["state_id"].isnull()[2] == True
        assert new_data["state_id"][3] == "in"
        assert len(pd.unique(new_data["state_id"])) == 4

    def test_fips_to_state_id(self):
        gmpr = GeoMapper()
        new_data = gmpr.fips_to_state_id(self.fips_data_2)
        assert new_data["state_id"][2] == "in"
        assert new_data.shape[0] == 3
        assert new_data["count"].sum() == self.fips_data_2["count"].sum()

    def test_fips_to_msa(self):
        gmpr = GeoMapper()
        new_data = gmpr.fips_to_msa(self.fips_data_3)
        assert new_data.shape[0] == 2
        assert new_data["msa"][0] == "10180"
        new_data = gmpr.fips_to_msa(self.fips_data_3, create_mega=True)
        assert new_data[["count"]].sum()[0] == self.fips_data_3["count"].sum()

    def test_zip_to_fips(self):
        gmpr = GeoMapper()
        new_data = gmpr.zip_to_fips(self.zip_data)
        assert new_data.shape[0] == 10
        assert (
            new_data[["count", "total"]].sum() - self.zip_data[["count", "total"]].sum()
        ).sum() < 1e-3

    def test_megacounty(self):
        gmpr = GeoMapper()
        new_data = gmpr.fips_to_megacounty(self.mega_data, 6, 50)
        assert (
            new_data[["count", "visits"]].sum()
            - self.mega_data[["count", "visits"]].sum()
        ).sum() < 1e-3

    def test_zip_to_hrr(self):
        gmpr = GeoMapper()
        new_data = gmpr.zip_to_hrr(self.zip_data)
        assert len(pd.unique(new_data["hrr"])) == 2
        assert np.allclose(
            new_data[["count", "total"]].sum(), self.zip_data[["count", "total"]].sum()
        )

    def test_jhu_uid_to_fips(self):
        gmpr = GeoMapper()
        self.jhu_uid_data["jhu_uid"] = self.jhu_uid_data["jhu_uid"].astype(str)
        new_data = gmpr.jhu_uid_to_fips(self.jhu_uid_data)
        assert not (new_data["fips"].astype(int) > 90000).any()
        assert new_data["total"].sum() == self.jhu_uid_data["total"].sum()

    def test_fips_to_zip(self):
        gmpr = GeoMapper()
        new_data = gmpr.fips_to_zip(self.fips_data_4)
        assert new_data["count"].sum() == self.fips_data_4["count"].sum()

    def test_fips_to_hrr(self):
        gmpr = GeoMapper()
        data = gmpr.convert_fips_to_hrr(self.fips_data_3)
        ind = self.fips_data_3["fips"].isin(data["fips"])
        data = self.fips_data_3[ind]
        new_data = gmpr.fips_to_hrr(self.fips_data_3)
        assert new_data.shape == (2, 4)
        assert new_data["count"].sum() == data["count"].sum()

    def test_zip_to_msa(self):
        gmpr = GeoMapper()
        new_data = gmpr.zip_to_msa(self.zip_data)
        assert new_data["msa"][2] == "46700"
        assert new_data.shape[0] == 6
        assert np.allclose(new_data["count"].sum(), self.zip_data["count"].sum())

    def test_zip_to_state_code(self):
        gmpr = GeoMapper()
        new_data = gmpr.zip_to_state_code(self.zip_data)
        assert new_data.shape[0] == 4
        assert np.allclose(new_data["count"].sum(), self.zip_data["count"].sum())

    def test_zip_to_state_id(self):
        gmpr = GeoMapper()
        new_data = gmpr.zip_to_state_id(self.zip_data)
        assert new_data.shape[0] == 4
        assert np.allclose(new_data["count"].sum(), self.zip_data["count"].sum())

    def test_add_population_column(self):
        gmpr = GeoMapper()
        new_data = gmpr.add_population_column(self.fips_data_3, "fips")
        assert new_data["population"].sum() == 274963
        new_data = gmpr.add_population_column(self.zip_data, "zip")
        assert new_data["population"].sum() == 274902

    def test_add_geocode(self):
        gmpr = GeoMapper()

        # fips -> zip
        new_data = gmpr.fips_to_zip(self.fips_data_3)
        new_data2 = gmpr.replace_geocode(self.fips_data_3, "fips", "zip")
        assert new_data.equals(new_data2)

        # fips -> hrr
        new_data = gmpr.fips_to_hrr(self.fips_data_3)
        new_data2 = gmpr.replace_geocode(self.fips_data_3, "fips", "hrr")
        new_data2 = new_data2[new_data.columns]
        assert np.allclose(
            new_data[["count", "total"]].values, new_data2[["count", "total"]].values
        )

        # fips -> msa
        new_data = gmpr.fips_to_msa(self.fips_data_3)
        new_data2 = gmpr.replace_geocode(self.fips_data_3, "fips", "msa")
        new_data2 = new_data2[new_data.columns]
        assert np.allclose(
            new_data[["count", "total"]].values, new_data2[["count", "total"]].values
        )

        # fips -> state_id
        new_data = gmpr.fips_to_state_id(self.fips_data_4)
        new_data2 = gmpr.replace_geocode(self.fips_data_4, "fips", "state_id")
        new_data2 = new_data2[new_data.columns]
        assert np.allclose(
            new_data[["count", "total"]].values, new_data2[["count", "total"]].values
        )

        # fips -> state_code
        new_data = gmpr.fips_to_state_code(self.fips_data_4)
        new_data2 = gmpr.replace_geocode(self.fips_data_4, "fips", "state_code")
        new_data2 = new_data2[new_data.columns]
        assert np.allclose(
            new_data[["count", "total"]].values, new_data2[["count", "total"]].values
        )

        # zip -> fips
        new_data = gmpr.zip_to_fips(self.zip_data)
        new_data2 = gmpr.replace_geocode(self.zip_data, "zip", "fips")
        new_data2 = new_data2[new_data.columns]
        assert new_data.equals(new_data2)

        # zip -> hrr
        new_data = gmpr.zip_to_hrr(self.zip_data)
        new_data2 = gmpr.replace_geocode(self.zip_data, "zip", "hrr")
        new_data2 = new_data2[new_data.columns]
        assert new_data.equals(new_data2)

        # zip -> msa
        new_data = gmpr.zip_to_msa(self.zip_data)
        new_data2 = gmpr.replace_geocode(self.zip_data, "zip", "msa")
        new_data2 = new_data2[new_data.columns]
        assert np.allclose(
            new_data[["count", "total"]].values, new_data2[["count", "total"]].values
        )

        # zip -> state_id
        new_data = gmpr.zip_to_state_id(self.zip_data)
        new_data2 = gmpr.replace_geocode(self.zip_data, "zip", "state_id")
        new_data2 = new_data2[new_data.columns]
        assert np.allclose(
            new_data[["count", "total"]].values, new_data2[["count", "total"]].values
        )

        # zip -> state_code
        new_data = gmpr.zip_to_state_code(self.zip_data)
        new_data2 = gmpr.replace_geocode(self.zip_data, "zip", "state_code")
        new_data2 = new_data2[new_data.columns]
        assert np.allclose(
            new_data[["count", "total"]].values, new_data2[["count", "total"]].values
        )

        # jhu_uid -> fips
        new_data = gmpr.jhu_uid_to_fips(self.jhu_uid_data)
        new_data2 = gmpr.replace_geocode(self.jhu_uid_data, "jhu_uid", "fips")
        new_data2 = new_data2[new_data.columns]
        assert np.allclose(
            new_data[["count", "total"]].values, new_data2[["count", "total"]].values
        )

        # jhu_uid -> fips BIG DATA
        # new_data = gmpr.jhu_uid_to_fips(self.jhu_big_data)
        # data_cols = new_data.filter(regex="[2,3]/\d{0,2}\/20").columns
        # new_data2 = gmpr.replace_geocode(self.jhu_big_data, "jhu_uid", "fips", data_cols=data_cols)
        # new_data2 = new_data2[new_data.columns]
        # assert np.allclose(new_data[["count", "total"]].values, new_data2[["count", "total"]].values)

        # state_code -> hhs
        new_data = gmpr.add_geocode(self.zip_data, "zip", "state_code")
        new_data2 = gmpr.add_geocode(new_data, "state_code", "hhs_region_number")
        assert new_data2["hhs_region_number"].unique().size == 2
