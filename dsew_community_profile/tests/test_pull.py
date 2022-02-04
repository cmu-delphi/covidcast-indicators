from collections import namedtuple
from datetime import date, datetime
from itertools import chain
import pandas as pd
import pytest
from unittest.mock import patch, Mock

from delphi_utils.geomap import GeoMapper

from delphi_dsew_community_profile.pull import DatasetTimes
from delphi_dsew_community_profile.pull import Dataset
from delphi_dsew_community_profile.pull import fetch_listing, nation_from_state, generate_prop_signal

example = namedtuple("example", "given expected")
        
class TestPull:
    def test_DatasetTimes(self):
        examples = [
            example(DatasetTimes("xyzzy", date(2021, 10, 30), date(2021, 10, 20), date(2021, 10, 22)),
                    DatasetTimes("xyzzy", date(2021, 10, 30), date(2021, 10, 20), date(2021, 10, 22))),
        ]
        for ex in examples:
            assert ex.given == ex.expected, "Equality"

        dt = DatasetTimes("xyzzy", date(2021, 10, 30), date(2021, 10, 20), date(2021, 10, 22))
        assert dt["positivity"] == date(2021, 10, 30), "positivity"
        assert dt["total"] == date(2021, 10, 20), "total"
        assert dt["confirmed covid-19 admissions"] == date(2021, 10, 22), "confirmed covid-19 admissions"
        with pytest.raises(ValueError):
            dt["xyzzy"]

    def test_DatasetTimes_from_header(self):
        examples = [
            example("TESTING: LAST WEEK (October 24-30, Test Volume October 20-26)",
                    DatasetTimes("last", date(2021, 10, 30), date(2021, 10, 26), None)),
            example("TESTING: PREVIOUS WEEK (October 24-30, Test Volume October 20-26)",
                    DatasetTimes("previous", date(2021, 10, 30), date(2021, 10, 26), None)),
            example("TESTING: LAST WEEK (October 24-November 30, Test Volume October 20-26)",
                    DatasetTimes("last", date(2021, 11, 30), date(2021, 10, 26), None)),
            example("VIRAL (RT-PCR) LAB TESTING: LAST WEEK (June 7-13, Test Volume June 3-9 )",
                    DatasetTimes("last", date(2021, 6, 13), date(2021, 6, 9), None)),
            example("VIRAL (RT-PCR) LAB TESTING: LAST WEEK (March 7-13)",
                    DatasetTimes("last", date(2021, 3, 13), date(2021, 3, 13), None)),
            example("HOSPITAL UTILIZATION: LAST WEEK (June 2-8)",
                    DatasetTimes("last", None, None, date(2021, 6, 8))),
            example("HOSPITAL UTILIZATION: LAST WEEK (June 28-July 8)",
                    DatasetTimes("last", None, None, date(2021, 7, 8)))
        ]
        for ex in examples:
            assert DatasetTimes.from_header(ex.given, date(2021, 12, 31)) == ex.expected, ex.given

        # test year boundary
        examples = [
            example("TESTING: LAST WEEK (October 24-30, Test Volume October 20-26)",
                    DatasetTimes("last", date(2020, 10, 30), date(2020, 10, 26), None)),
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

    @patch('requests.get')
    @patch('os.path.exists')
    def test_fetch_listing(self, mock_listing, mock_exists):
        inst = namedtuple("attachment", "assetId filename publish cache")
        instances = list(chain(*[
            [
                inst(f"{i}", f"2021010{i}.xlsx", date(2021, 1, i), f"{i}---2021010{i}.xlsx"),
                inst(f"p{i}", f"2021010{i}.pdf", date(2021, 1, i), f"p{i}---2021010{i}.pdf"),
            ]
            for i in [1, 2, 3, 4, 5]
        ]))

        mock_listing.return_value = Mock()
        mock_listing.return_value.json = Mock(
            return_value = {
                'metadata': {
                    'attachments': [
                        {"assetId": i.assetId, "filename": i.filename}
                        for i in instances
                    ]
                }
            }
        )

        mock_exists.reset_mock(return_value=False)

        def as_listing(instance):
            return {
                "assetId": instance.assetId,
                "filename": instance.filename,
                "cached_filename": instance.cache,
                "publish_date": instance.publish
            }
        ex = example(
            {'indicator':{'reports':'new'}},
            [
                as_listing(instance)
                for i, instance in filter(lambda x: x[0]%2 == 0, enumerate(instances))
            ]
        )

        for actual, expected in zip(fetch_listing(ex.given), ex.expected):
            assert actual == expected

    def test_nation_from_state(self):
        geomapper = GeoMapper()
        state_pop = geomapper.get_crosswalk("state_id", "pop")

        test_df = pd.DataFrame({
                'state_id': ['pa', 'wv'],
                'timestamp': [datetime(year=2020, month=1, day=1)]*2,
                'val': [15., 150.],
                'se': [None, None],
                'sample_size': [None, None],})

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
                'sample_size': [None],}),
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
                'sample_size': [None],}),
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
