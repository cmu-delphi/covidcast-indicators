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
    fips_data_5 = pd.DataFrame(
        {
            "fips": [1123, 48253, 72003, 18181],
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
    mega_data_2 = pd.concat(
        (
            pd.DataFrame(
                {
                    "fips": ["01001"] * len(jan_month),
                    "date": jan_month,
                    "count": np.arange(len(jan_month)),
                    "_thr_col_roll": np.arange(len(jan_month)),
                }
            ),
            pd.DataFrame(
                {
                    "fips": [11001] * len(jan_month),
                    "date": jan_month,
                    "count": np.arange(len(jan_month)),
                    "_thr_col_roll": np.arange(len(jan_month)),
                }
            ),
        )
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
    def test_crosswalks(self):
        # These tests ensure that the one-to-many crosswalks have properly normalized weights
        gmpr = GeoMapper()
        # FIPS -> HRR is allowed to be an incomplete mapping, since only a fraction of a FIPS
        # code can not belong to an HRR
        cw = gmpr._load_crosswalk(from_code="fips", to_code="hrr")
        assert (
            cw.groupby("fips")["weight"].sum().round(5).ge(0.95).all()
        )  # some weight discrepancy is fine for HRR
        cw = gmpr._load_crosswalk(from_code="fips", to_code="zip")
        assert cw.groupby("fips")["weight"].sum().round(5).eq(1.0).all()
        cw = gmpr._load_crosswalk(from_code="jhu_uid", to_code="fips")
        assert cw.groupby("jhu_uid")["weight"].sum().round(5).eq(1.0).all()
        cw = gmpr._load_crosswalk(from_code="zip", to_code="fips")
        assert cw.groupby("zip")["weight"].sum().round(5).eq(1.0).all()
        # weight discrepancy is fine for MSA, for the same reasons as HRR
        # cw = gmpr.load_crosswalk(from_code="zip", to_code="msa")
        # assert cw.groupby("zip")["weight"].sum().round(5).eq(1.0).all()
        cw = gmpr._load_crosswalk(from_code="zip", to_code="state")
        assert cw.groupby("zip")["weight"].sum().round(5).eq(1.0).all()
        cw = gmpr._load_crosswalk(from_code="zip", to_code="hhs")
        assert cw.groupby("zip")["weight"].sum().round(5).eq(1.0).all()


    def test_load_zip_fips_table(self):
        gmpr = GeoMapper()
        fips_data = gmpr._load_crosswalk(from_code="zip", to_code="fips")
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
        assert np.allclose(jhu_data.groupby("jhu_uid").sum(), 1.0)

    def test_load_zip_hrr_table(self):
        gmpr = GeoMapper()
        zip_data = gmpr._load_crosswalk(from_code="zip", to_code="hrr")
        assert pd.api.types.is_string_dtype(zip_data["zip"])
        assert pd.api.types.is_string_dtype(zip_data["hrr"])

    def test_megacounty(self):
        gmpr = GeoMapper()
        new_data = gmpr.fips_to_megacounty(self.mega_data, 6, 50)
        assert (
            new_data[["count", "visits"]].sum()
            - self.mega_data[["count", "visits"]].sum()
        ).sum() < 1e-3
        with pytest.raises(ValueError):
            new_data = gmpr.megacounty_creation(
                self.mega_data_2, 6, 50, thr_col="_thr_col_roll"
            )
        new_data = gmpr.fips_to_megacounty(
            self.mega_data, 6, 50, count_cols=["count", "visits"]
        )
        assert (
            new_data[["count"]].sum() - self.mega_data[["count"]].sum()
        ).sum() < 1e-3

    def test_add_population_column(self):
        gmpr = GeoMapper()
        new_data = gmpr.add_population_column(self.fips_data_3, "fips")
        assert new_data.shape == (5, 5)
        new_data = gmpr.add_population_column(self.zip_data, "zip")
        assert new_data.shape == (6, 5)
        with pytest.raises(ValueError):
            new_data = gmpr.add_population_column(self.zip_data, "hrr")
        new_data = gmpr.add_population_column(self.fips_data_5, "fips")
        assert new_data.shape == (4, 5)

    def test_add_geocode(self):
        gmpr = GeoMapper()

        # state_code -> nation
        new_data = gmpr.add_geocode(self.zip_data, "zip", "state_code")
        new_data2 = gmpr.add_geocode(new_data, "state_code", "nation")
        assert new_data2["nation"].unique()[0] == "us"

        # state_code -> hhs
        new_data = gmpr.add_geocode(self.zip_data, "zip", "state_code")
        new_data2 = gmpr.add_geocode(new_data, "state_code", "hhs")
        assert new_data2["hhs"].unique().size == 2

        # state_name -> state_id
        new_data = gmpr.replace_geocode(self.zip_data, "zip", "state_name")
        new_data2 = gmpr.add_geocode(new_data, "state_name", "state_id")
        assert new_data2.shape == (4, 5)
        new_data2 = gmpr.replace_geocode(new_data, "state_name", "state_id", new_col="abbr")
        assert "abbr" in new_data2.columns

        # fips -> nation
        new_data = gmpr.replace_geocode(self.fips_data_5, "fips", "nation", new_col="NATION")
        pd.testing.assert_frame_equal(
            new_data,
            pd.DataFrame().from_dict(
                {
                    "date": {0: pd.Timestamp("2018-01-01 00:00:00")},
                    "NATION": {0: "us"},
                    "count": {0: 10024.0},
                    "total": {0: 100006.0},
                }
            )
        )

        # zip -> nation
        new_data = gmpr.replace_geocode(self.zip_data, "zip", "nation")
        pd.testing.assert_frame_equal(
            new_data,
            pd.DataFrame().from_dict(
                {
                    "date": {
                        0: pd.Timestamp("2018-01-01"),
                        1: pd.Timestamp("2018-01-03"),
                    },
                    "nation": {0: "us", 1: "us"},
                    "count": {0: 900, 1: 886},
                    "total": {0: 1800, 1: 1772},
                }
            )
        )

        # hrr -> nation
        with pytest.raises(ValueError):    
            new_data = gmpr.replace_geocode(self.zip_data, "zip", "hrr")
            new_data2 = gmpr.replace_geocode(new_data, "hrr", "nation")

        # fips -> hrr (dropna=True/False check)
        assert not gmpr.add_geocode(self.fips_data_3, "fips", "hrr").isna().any().any()
        assert gmpr.add_geocode(self.fips_data_3, "fips", "hrr", dropna=False).isna().any().any()

        # fips -> zip (date_col=None chech)
        new_data = gmpr.replace_geocode(self.fips_data_5.drop(columns=["date"]), "fips", "hrr", date_col=None)
        pd.testing.assert_frame_equal(
            new_data,
            pd.DataFrame().from_dict(
                {
                    'hrr': {0: '1', 1: '183', 2: '184', 3: '382', 4: '7'},
                    'count': {0: 1.772347174163783, 1: 7157.392403522299, 2: 2863.607596477701, 3: 1.0, 4: 0.22765282583621685},
                    'total': {0: 3.544694348327566, 1: 71424.64801363471, 2: 28576.35198636529, 3: 1.0, 4: 0.4553056516724337}
                }
            )
        )

        # fips -> hhs
        new_data = gmpr.replace_geocode(self.fips_data_3.drop(columns=["date"]),
                                        "fips", "hhs", date_col=None)
        pd.testing.assert_frame_equal(
            new_data,
            pd.DataFrame().from_dict(
                {
                    "hhs": {0: "2", 1: "6"},
                    "count": {0: 12, 1: 6},
                    "total": {0: 111, 1: 13}
                }
            )
        )

        # zip -> hhs
        new_data = gmpr.replace_geocode(self.zip_data, "zip", "hhs")
        new_data = new_data.round(10)  # get rid of a floating point error with 99.00000000000001
        pd.testing.assert_frame_equal(
            new_data,
            pd.DataFrame().from_dict(
                {
                    "date": {0: pd.Timestamp("2018-01-01"), 1: pd.Timestamp("2018-01-01"),
                             2: pd.Timestamp("2018-01-03"), 3: pd.Timestamp("2018-01-03")},
                    "hhs": {0: "5", 1: "9", 2: "5", 3: "9"},
                    "count": {0: 99.0, 1: 801.0, 2: 100.0, 3: 786.0},
                    "total": {0: 198.0, 1: 1602.0, 2: 200.0, 3: 1572.0}
                }
            )
        )

    def test_get_geos(self):
        gmpr = GeoMapper()
        assert gmpr.get_geo_values("nation") == {"us"}
        assert gmpr.get_geo_values("hhs") == set(str(i) for i in range(1, 11))
        assert len(gmpr.get_geo_values("fips")) == 3274
        assert len(gmpr.get_geo_values("state_id")) == 60
        assert len(gmpr.get_geo_values("zip")) == 32976
