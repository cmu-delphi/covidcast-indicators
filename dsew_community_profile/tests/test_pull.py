from collections import namedtuple
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from itertools import chain
from typing import Any, Dict, List, Union
import pandas as pd
from pandas.testing import assert_frame_equal
import numpy as np
import pytest
from unittest.mock import patch, Mock

from delphi_utils.geomap import GeoMapper

from delphi_dsew_community_profile.pull import (
    DatasetTimes, Dataset,
    fetch_listing, nation_from_state, generate_prop_signal,
    std_err, add_max_ts_col, unify_testing_sigs, interpolate_missing_values,
    extend_listing_for_interp
)


example = namedtuple("example", "given expected")

def _assert_frame_equal(df1, df2, index_cols: List[str] = None):
    # Ensure same columns present.
    assert set(df1.columns) == set(df2.columns)
    # Ensure same column order.
    df1 = df1[df1.columns]
    df2 = df2[df1.columns]
    # Ensure same row order by using a common index and sorting.
    df1 = df1.set_index(index_cols).sort_index()
    df2 = df2.set_index(index_cols).sort_index()
    return assert_frame_equal(df1, df2)

def _set_df_dtypes(df: pd.DataFrame, dtypes: Dict[str, Any]) -> pd.DataFrame:
    df = df.copy()
    for k, v in dtypes.items():
        if k in df.columns:
            df[k] = df[k].astype(v)
    return df


class TestPull:
    def test_DatasetTimes(self):
        examples = [
            example(DatasetTimes("xyzzy", date(2021, 10, 30), date(2021, 10, 20), date(2021, 10, 22), date(2021, 10, 23), date(2021, 10, 24)),
                    DatasetTimes("xyzzy", date(2021, 10, 30), date(2021, 10, 20), date(2021, 10, 22), date(2021, 10, 23), date(2021, 10, 24))),
        ]
        for ex in examples:
            assert ex.given == ex.expected, "Equality"

        dt = DatasetTimes("xyzzy", date(2021, 10, 30), date(2021, 10, 20), date(2021, 10, 22), date(2021, 10, 23), date(2021, 10, 24))
        assert dt["positivity"] == date(2021, 10, 30), "positivity"
        assert dt["total"] == date(2021, 10, 20), "total"
        assert dt["confirmed covid-19 admissions"] == date(2021, 10, 22), "confirmed covid-19 admissions"
        assert dt["doses administered"] == date(2021, 10, 24), "doses administered"
        assert dt["fully vaccinated"] == date(2021, 10, 23), "fully vaccinated"
        with pytest.raises(ValueError):
            dt["xyzzy"]

    def test_DatasetTimes_from_header(self):
        examples = [
            example("TESTING: LAST WEEK (October 24-30, Test Volume October 20-26)",
                    DatasetTimes("last", date(2021, 10, 30), date(2021, 10, 26), None, None, None)),
            example("TESTING: PREVIOUS WEEK (October 24-30, Test Volume October 20-26)",
                    DatasetTimes("previous", date(2021, 10, 30), date(2021, 10, 26), None, None, None)),
            example("TESTING: LAST WEEK (October 24-November 30, Test Volume October 20-26)",
                    DatasetTimes("last", date(2021, 11, 30), date(2021, 10, 26), None, None, None)),
            example("VIRAL (RT-PCR) LAB TESTING: LAST WEEK (June 7-13, Test Volume June 3-9 )",
                    DatasetTimes("last", date(2021, 6, 13), date(2021, 6, 9), None, None, None)),
            example("VIRAL (RT-PCR) LAB TESTING: LAST WEEK (March 7-13)",
                    DatasetTimes("last", date(2021, 3, 13), date(2021, 3, 13), None, None, None)),
            example("HOSPITAL UTILIZATION: LAST WEEK (June 2-8)",
                    DatasetTimes("last", None, None, date(2021, 6, 8), None, None)),
            example("HOSPITAL UTILIZATION: LAST WEEK (June 28-July 8)",
                    DatasetTimes("last", None, None, date(2021, 7, 8), None, None)),
            example("COVID-19 VACCINATION DATA: CUMULATIVE (January 25)",
                    DatasetTimes("", None, None, None, date(2021, 1, 25), None)),
            example("COVID-19 VACCINATION DATA: LAST WEEK (January 25-31)",
                    DatasetTimes("last", None, None,  None, None, date(2021, 1, 25)))
        ]
        for ex in examples:
            assert DatasetTimes.from_header(ex.given, date(2021, 12, 31)) == ex.expected, ex.given

        # test year boundary
        examples = [
            example("TESTING: LAST WEEK (October 24-30, Test Volume October 20-26)",
                    DatasetTimes("last", date(2020, 10, 30), date(2020, 10, 26), None, None, None)),
        ]
        for ex in examples:
            assert DatasetTimes.from_header(ex.given, date(2021, 1, 1)) == ex.expected, ex.given

    def test_Dataset_skip_overheader(self):
        examples = [
            example("TESTING: LAST WEEK (October 24-30, Test Volume October 20-26)",
                    False),
            example("TESTING: PREVIOUS WEEK (October 17-23, Test Volume October 13-19)",
                    False),
            example("VIRAL (RT-PCR) LAB TESTING: LAST WEEK (August 24-30, Test Volume August 20-26)",
                    False),
            example("VIRAL (RT-PCR) LAB TESTING: PREVIOUS WEEK (August 17-23, Test Volume August 13-19)",
                    False),
            example("TESTING: % CHANGE FROM PREVIOUS WEEK",
                    True),
            example("VIRAL (RT-PCR) LAB TESTING: % CHANGE FROM PREVIOUS WEEK",
                    True),
            example("TESTING: DEMOGRAPHIC DATA",
                    True),
            example("HOSPITAL UTILIZATION: LAST WEEK (January 2-8)",
                    False),
            example("HOSPITAL UTILIZATION: CHANGE FROM PREVIOUS WEEK",
                    True),
            example("HOSPITAL UTILIZATION: DEMOGRAPHIC DATA",
                    True),
            example("COVID-19 VACCINATION DATA: CUMULATIVE (January 25)",
                    False),
            example("COVID-19 VACCINATION DATA: LAST WEEK (January 25-31)",
                    False),
            example("COVID-19 VACCINATION DATA: DEMOGRAPHIC DATA",
                    True)
        ]
        for ex in examples:
            assert Dataset.skip_overheader(ex.given) == ex.expected, ex.given
    def test_Dataset_retain_header(self):
        examples = [
            example("Total NAATs - last 7 days (may be an underestimate due to delayed reporting)",
                    True),
            example("Total NAATs - previous 7 days (may be an underestimate due to delayed reporting)",
                    True),
            example("NAAT positivity rate - last 7 days (may be an underestimate due to delayed reporting)",
                    True),
            example("NAAT positivity rate - previous 7 days (may be an underestimate due to delayed reporting)",
                    True),
            example("NAAT positivity rate - absolute change (may be an underestimate due to delayed reporting)",
                    False),
            example("NAAT positivity rate - last 7 days - ages <5",
                    False),
            example("Total RT-PCR diagnostic tests - last 7 days (may be an underestimate due to delayed reporting)",
                    True),
            example("Viral (RT-PCR) lab test positivity rate - last 7 days (may be an underestimate due to delayed reporting)",
                    True),
            example("RT-PCR tests per 100k - last 7 days (may be an underestimate due to delayed reporting)",
                    False),
            example("Confirmed COVID-19 admissions - last 7 days",
                    True),
            example("Confirmed COVID-19 admissions - percent change",
                    False),
            example("Confirmed COVID-19 admissions - last 7 days - ages <18",
                    False),
            example("Confirmed COVID-19 admissions - last 7 days - age unknown",
                    False),
            example("Confirmed COVID-19 admissions per 100 inpatient beds - last 7 days",
                    False)
        ]
        for ex in examples:
            assert Dataset.retain_header(ex.given) == ex.expected, ex.given
            
    def test_Dataset_parse_sheet(self):
        # TODO
        pass

    def test_fetch_listing(self):
        inst = namedtuple("attachment", "assetId filename publish cache")
        instances = list(chain(*[
            [
                inst(f"{i}", f"2021010{i}.xlsx", date(2021, 1, i), f"2021010{i}--{i}.xlsx"),
                inst(f"p{i}", f"2021010{i}.pdf", date(2021, 1, i), f"2021010{i}--p{i}.pdf"),
            ]
            for i in [1, 2, 3, 4, 5]
        ]))

        # Solution from https://stackoverflow.com/questions/15753390/
        #how-can-i-mock-requests-and-the-response
        def mocked_requests_get(*args, **kwargs):
            class MockResponse:
                def __init__(self, json_data):
                    self.json_data = json_data

                def json(self):
                    return self.json_data

            return MockResponse({
                        'metadata': {
                            'attachments': [
                                {"assetId": i.assetId, "filename": i.filename}
                                for i in instances
                            ]
                        }
                    }
                )

        def as_listing(instance):
            return {
                "assetId": instance.assetId,
                "filename": instance.filename,
                "cached_filename": instance.cache,
                "publish_date": instance.publish
            }
        ex = example(
            {'indicator':{'reports':'new', 'input_cache':''}},
            [
                as_listing(instance)
                for i, instance in filter(lambda x: x[0]%2 == 0, enumerate(instances))
            ]
        )

        with patch('requests.get', side_effect=mocked_requests_get):
            with patch('os.path.exists', return_value=False):
                for actual, expected in zip(fetch_listing(ex.given), ex.expected):
                    assert actual == expected

            with patch('os.path.exists', return_value=True):
                assert fetch_listing(ex.given) == []

    def test_nation_from_state(self):
        geomapper = GeoMapper()
        state_pop = geomapper.get_crosswalk("state_id", "pop")

        test_df = pd.DataFrame({
                'state_id': ['pa', 'wv'],
                'timestamp': [datetime(year=2020, month=1, day=1)]*2,
                'val': [15., 150.],
                'se': [None, None],
                'sample_size': [None, None],
                'publish_date': [datetime(year=2020, month=1, day=1)]*2,})

        pa_pop = int(state_pop.loc[state_pop.state_id == "pa", "pop"])
        wv_pop = int(state_pop.loc[state_pop.state_id == "wv", "pop"])
        tot_pop = pa_pop + wv_pop

        assert True, nation_from_state(
                test_df.copy(),
                "total",
                geomapper
            )
        pd.testing.assert_frame_equal(
            nation_from_state(
                test_df.copy(),
                "total",
                geomapper
            ),
            pd.DataFrame({
                'geo_id': ['us'],
                'timestamp': [datetime(year=2020, month=1, day=1)],
                'val': [15. + 150.],
                'se': [None],
                'sample_size': [None],
                'publish_date': [datetime(year=2020, month=1, day=1)],}),
            check_like=True
        )

        pd.testing.assert_frame_equal(
            nation_from_state(
                test_df.copy(),
                "positivity",
                geomapper
            ),
            pd.DataFrame({
                'geo_id': ['us'],
                'timestamp': [datetime(year=2020, month=1, day=1)],
                'val': [15*pa_pop/tot_pop + 150*wv_pop/tot_pop],
                'se': [None],
                'sample_size': [None],
                'publish_date': [datetime(year=2020, month=1, day=1)],}),
            check_like=True
        )

    def test_generate_prop_signal_msa(self):
        geomapper = GeoMapper()
        county_pop = geomapper.get_crosswalk("fips", "pop")
        county_msa = geomapper.get_crosswalk("fips", "msa")
        msa_pop = county_pop.merge(county_msa, on="fips", how="inner").groupby("msa").sum().reset_index()

        test_df = pd.DataFrame({
                'geo_id': ['35620', '31080'],
                'timestamp': [datetime(year=2020, month=1, day=1)]*2,
                'val': [15., 150.],
                'se': [None, None],
                'sample_size': [None, None],})

        nyc_pop = int(msa_pop.loc[msa_pop.msa == "35620", "pop"])
        la_pop = int(msa_pop.loc[msa_pop.msa == "31080", "pop"])

        expected_df = pd.DataFrame({
                'geo_id': ['35620', '31080'],
                'timestamp': [datetime(year=2020, month=1, day=1)]*2,
                'val': [15. / nyc_pop * 100000, 150. / la_pop * 100000],
                'se': [None, None],
                'sample_size': [None, None],})

        pd.testing.assert_frame_equal(
            generate_prop_signal(
                test_df.copy(),
                "msa",
                geomapper
            ),
            expected_df,
            check_like=True
        )
    def test_generate_prop_signal_non_msa(self):
        geomapper = GeoMapper()

        geos = {
            "state": {
                "code_name": "state_id",
                "geo_names": ['pa', 'wv']
            },
            "county": {
                "code_name": "fips",
                "geo_names": ['36061', '06037']
            },
            # nation uses the same logic path so no need to test separately
            "hhs": {
                "code_name": "hhs",
                "geo_names": ["1", "4"]
            }
        }

        for geo, settings in geos.items():
            geo_pop = geomapper.get_crosswalk(settings["code_name"], "pop")

            test_df = pd.DataFrame({
                    'geo_id': settings["geo_names"],
                    'timestamp': [datetime(year=2020, month=1, day=1)]*2,
                    'val': [15., 150.],
                    'se': [None, None],
                    'sample_size': [None, None],})

            pop1 = int(geo_pop.loc[geo_pop[settings["code_name"]] == settings["geo_names"][0], "pop"])
            pop2 = int(geo_pop.loc[geo_pop[settings["code_name"]] == settings["geo_names"][1], "pop"])

            expected_df = pd.DataFrame({
                    'geo_id': settings["geo_names"],
                    'timestamp': [datetime(year=2020, month=1, day=1)]*2,
                    'val': [15. / pop1 * 100000, 150. / pop2 * 100000],
                    'se': [None, None],
                    'sample_size': [None, None],})

            pd.testing.assert_frame_equal(
                generate_prop_signal(
                    test_df.copy(),
                    geo,
                    geomapper
                ),
                expected_df,
                check_like=True
            )

    def test_unify_testing_sigs(self):
        positivity_df = pd.DataFrame({
            'geo_id': ["ca", "ca", "fl", "fl"],
            'timestamp': [datetime(2021, 10, 27), datetime(2021, 10, 20)]*2,
            'val': [0.2, 0.34, 0.7, 0.01],
            'se': [None] * 4,
            'sample_size': [None] * 4,
            'publish_date': [datetime(2021, 10, 30)]*4,
        })
        base_volume_df = pd.DataFrame({
            'geo_id': ["ca", "ca", "fl", "fl"],
            'timestamp': [datetime(2021, 10, 23), datetime(2021, 10, 16)]*2,
            'val': [None] * 4,
            'se': [None] * 4,
            'sample_size': [None] * 4,
            'publish_date': [datetime(2021, 10, 30)]*4,
        })

        examples = [
            example(
                [positivity_df, base_volume_df.assign(val = [101, 102, 103, 104])],
                positivity_df.assign(
                    sample_size = [101, 102, 103, 104],
                    se = lambda df: np.sqrt(df.val * (1 - df.val) / df.sample_size)
                )
            ), # No filtering
            example(
                [positivity_df, base_volume_df.assign(val = [110, 111, 112, 113]).iloc[::-1]],
                positivity_df.assign(
                    sample_size = [110, 111, 112, 113],
                    se = lambda df: np.sqrt(df.val * (1 - df.val) / df.sample_size)
                )
            ), # No filtering, volume df in reversed order
            example(
                [positivity_df, base_volume_df.assign(val = [100, 5, 1, 6])],
                positivity_df.assign(
                    sample_size = [100, 5, 1, 6]
                ).iloc[[0, 3]].assign(
                    se = lambda df: np.sqrt(df.val * (1 - df.val) / df.sample_size)
                )
            )
        ]
        for ex in examples:
            pd.testing.assert_frame_equal(unify_testing_sigs(ex.given[0], ex.given[1]), ex.expected)

        with pytest.raises(AssertionError):
            # Inputs have different numbers of rows.
            unify_testing_sigs(positivity_df, positivity_df.head(n=1))

    def test_add_max_ts_col(self):
        input_df = pd.DataFrame({
            'geo_id': ["ca", "ca", "fl", "fl"],
            'timestamp': [datetime(2021, 10, 27), datetime(2021, 10, 20)]*2,
            'val': [1, 2, 3, 4],
            'se': [None] * 4,
            'sample_size': [None] * 4,
            'publish_date': [datetime(2021, 10, 30)]*4,
        })
        examples = [
            example(input_df, input_df.assign(is_max_group_ts = [True, False, True, False])),
        ]
        for ex in examples:
            pd.testing.assert_frame_equal(add_max_ts_col(ex.given), ex.expected)

        with pytest.raises(AssertionError):
            # Input df has 2 timestamps per geo id-publish date combination, but not 2 unique timestamps.
            add_max_ts_col(
                pd.DataFrame({
                    'geo_id': ["ca", "ca", "fl", "fl"],
                    'timestamp': [datetime(2021, 10, 27)] * 4,
                    'val': [1, 2, 3, 4],
                    'se': [None] * 4,
                    'sample_size': [None] * 4,
                    'publish_date': [datetime(2021, 10, 30)] * 4,
                })
            )
        with pytest.raises(AssertionError):
            # Input df has more than 2 timestamps per geo id-publish date combination.
            add_max_ts_col(
                pd.DataFrame({
                    'geo_id': ["ca", "ca", "ca", "fl", "fl", "fl"],
                    'timestamp': [datetime(2021, 10, 27)] * 6,
                    'val': [1, 2, 3, 4, 5, 6],
                    'se': [None] * 6,
                    'sample_size': [None] * 6,
                    'publish_date': [datetime(2021, 10, 30)] * 6,
                })
            )

        try:
            # Input df has fewer than 2 timestamps per geo id-publish date
            # combination. This should not raise an exception.
            add_max_ts_col(
                pd.DataFrame({
                    'geo_id': ["ca", "fl"],
                    'timestamp': [datetime(2021, 10, 27)] * 2,
                    'val': [1, 2],
                    'se': [None] * 2,
                    'sample_size': [None] * 2,
                    'publish_date': [datetime(2021, 10, 30)] * 2,
                })
            )
        except AssertionError as e:
            assert False, f"'add_max_ts_col' raised exception: {e}"

        try:
            # Input df has 2 unique timestamps per geo id-publish date
            # combination. This should not raise an exception.
            add_max_ts_col(
                pd.DataFrame({
                    'geo_id': ["ca", "ca", "fl", "fl"],
                    'timestamp': [datetime(2021, 10, 27), datetime(2021, 10, 20)] * 2,
                    'val': [1, 2, 3, 4],
                    'se': [None] * 4,
                    'sample_size': [None] * 4,
                    'publish_date': [datetime(2021, 10, 30)] * 4,
                })
            )
        except AssertionError as e:
            assert False, f"'add_max_ts_col' raised exception: {e}"

    def test_std_err(self):
        df = pd.DataFrame({
            "val": [0, 0.5, 0.4, 0.3, 0.2, 0.1],
            "sample_size": [2, 2, 5, 10, 20, 50]
        })

        expected_se = np.sqrt(df.val * (1 - df.val) / df.sample_size)
        se = std_err(df)

        assert (se >= 0).all()
        assert not np.isnan(se).any()
        assert not np.isinf(se).any()
        assert np.allclose(se, expected_se, equal_nan=True)
        with pytest.raises(AssertionError):
            std_err(
                pd.DataFrame({
                    "val": [0, 0.5, 0.4, 0.3, 0.2, 0.1],
                    "sample_size": [2, 2, 5, 10, 20, 0]
                })
            )

    def test_interpolation(self):
        DTYPES = {"geo_id": str, "timestamp": "datetime64[ns]", "val": float, "se": float, "sample_size": float, "publish_date": "datetime64[ns]"}
        line = lambda x: 3 * x + 5

        sig1 = _set_df_dtypes(pd.DataFrame({
            "geo_id": "1",
            "timestamp": pd.date_range("2022-01-01", "2022-01-10"),
            "val": [line(i) for i in range(2, 12)],
            "se": [line(i) for i in range(1, 11)],
            "sample_size": [line(i) for i in range(0, 10)],
            "publish_date": pd.to_datetime("2022-01-10")
        }), dtypes=DTYPES)
        # A linear signal missing two days which should be filled exactly by the linear interpolation.
        missing_sig1 = sig1[(sig1.timestamp <= "2022-01-05") | (sig1.timestamp >= "2022-01-08")]

        sig2 = sig1.copy()
        sig2["geo_id"] = "2"
        # A linear signal missing everything but the end points, should be filled exactly by linear interpolation.
        missing_sig2 = sig2[(sig2.timestamp == "2022-01-01") | (sig2.timestamp == "2022-01-10")]

        sig3 = _set_df_dtypes(pd.DataFrame({
            "geo_id": "3",
            "timestamp": pd.date_range("2022-01-01", "2022-01-10"),
            "val": None,
            "se": [line(i) for i in range(1, 11)],
            "sample_size": [line(i) for i in range(0, 10)],
            "publish_date": pd.to_datetime("2022-01-10")
        }), dtypes=DTYPES)
        # A signal missing everything, should be dropped since it's all NAs.
        missing_sig3 = sig3[(sig3.timestamp <= "2022-01-05") | (sig3.timestamp >= "2022-01-08")]

        sig4 = _set_df_dtypes(pd.DataFrame({
            "geo_id": "4",
            "timestamp": pd.date_range("2022-01-01", "2022-01-10"),
            "val": [None] * 9 + [10.0],
            "se": [line(i) for i in range(1, 11)],
            "sample_size": [line(i) for i in range(0, 10)],
            "publish_date": pd.to_datetime("2022-01-10")
        }), dtypes=DTYPES)
        # A signal missing everything except for one point, should output a reduced range without NAs.
        missing_sig4 = sig4[(sig4.timestamp <= "2022-01-05") | (sig4.timestamp >= "2022-01-08")]

        missing_dfs = [missing_sig1, missing_sig2, missing_sig3, missing_sig4]
        interpolated_dfs1 = interpolate_missing_values({("src", "sig", False): pd.concat(missing_dfs)})
        expected_dfs = pd.concat([sig1, sig2, sig4.loc[9:]])
        _assert_frame_equal(interpolated_dfs1[("src", "sig", False)], expected_dfs, index_cols=["geo_id", "timestamp"])

    def test_interpolation_object_type(self):
        DTYPES = {"geo_id": str, "timestamp": "datetime64[ns]", "val": float, "se": float, "sample_size": float, "publish_date": "datetime64[ns]"}
        line = lambda x: 3 * x + 5

        sig1 = _set_df_dtypes(pd.DataFrame({
            "geo_id": "1",
            "timestamp": pd.date_range("2022-01-01", "2022-01-10"),
            "val": [line(i) for i in range(2, 12)],
            "se": [line(i) for i in range(1, 11)],
            "sample_size": [line(i) for i in range(0, 10)],
            "publish_date": pd.to_datetime("2022-01-10")
        }), dtypes=DTYPES)
        # A linear signal missing two days which should be filled exactly by the linear interpolation.
        missing_sig1 = sig1[(sig1.timestamp <= "2022-01-05") | (sig1.timestamp >= "2022-01-08")]
        # set all columns to object type to simulate the miscast we sometimes see when combining dfs
        missing_sig1 = _set_df_dtypes(missing_sig1, {key: object for key in DTYPES.keys()})

        interpolated_dfs1 = interpolate_missing_values({("src", "sig", False): missing_sig1})
        expected_dfs = pd.concat([sig1])
        _assert_frame_equal(interpolated_dfs1[("src", "sig", False)], expected_dfs, index_cols=["geo_id", "timestamp"])

    @patch("delphi_dsew_community_profile.pull.INTERP_LENGTH", 2)
    def test_extend_listing(self):
        listing = [
            {"publish_date": date(2020, 1, 20) - timedelta(days=i)}
            for i in range(20)
        ]
        examples = [
            # single range
            example(
                [{"publish_date": date(2020, 1, 20)}],
                [{"publish_date": date(2020, 1, 20)}, {"publish_date": date(2020, 1, 19)}]
            ),
            # disjoint ranges
            example(
                [{"publish_date": date(2020, 1, 20)}, {"publish_date": date(2020, 1, 10)}],
                [{"publish_date": date(2020, 1, 20)}, {"publish_date": date(2020, 1, 19)},
                 {"publish_date": date(2020, 1, 10)}, {"publish_date": date(2020, 1, 9)}]
            ),
            # conjoined ranges
            example(
                [{"publish_date": date(2020, 1, 20)}, {"publish_date": date(2020, 1, 19)}],
                [{"publish_date": date(2020, 1, 20)}, {"publish_date": date(2020, 1, 19)}, {"publish_date": date(2020, 1, 18)}]
            ),
            # empty keep list
            example(
                [],
                []
            )
        ]
        for ex in examples:
            assert extend_listing_for_interp(ex.given, listing) == ex.expected, ex.given
