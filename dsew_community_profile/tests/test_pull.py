from collections import namedtuple
from datetime import date, datetime
from itertools import chain
import pandas as pd
import pytest
from unittest.mock import patch, Mock

from delphi_utils.geomap import GeoMapper

from delphi_dsew_community_profile.pull import DatasetTimes
from delphi_dsew_community_profile.pull import Dataset
from delphi_dsew_community_profile.pull import fetch_listing, nation_from_state

example = namedtuple("example", "given expected")
        
class TestPull:
    def test_DatasetTimes(self):
        examples = [
            example(DatasetTimes("xyzzy", date(2021, 10, 30), date(2021, 10, 20)),
                    DatasetTimes("xyzzy", date(2021, 10, 30), date(2021, 10, 20))),
        ]
        for ex in examples:
            assert ex.given == ex.expected, "Equality"

        dt = DatasetTimes("xyzzy", date(2021, 10, 30), date(2021, 10, 20))        
        assert dt["positivity"] == date(2021, 10, 30), "positivity"
        assert dt["total"] == date(2021, 10, 20), "total"
        with pytest.raises(ValueError):
            dt["xyzzy"]

    def test_DatasetTimes_from_header(self):
        examples = [
            example("TESTING: LAST WEEK (October 24-30, Test Volume October 20-26)",
                    DatasetTimes("last", date(2021, 10, 30), date(2021, 10, 26))),
            example("TESTING: PREVIOUS WEEK (October 24-30, Test Volume October 20-26)",
                    DatasetTimes("previous", date(2021, 10, 30), date(2021, 10, 26))),
            example("TESTING: LAST WEEK (October 24-November 30, Test Volume October 20-26)",
                    DatasetTimes("last", date(2021, 11, 30), date(2021, 10, 26))),
            example("VIRAL (RT-PCR) LAB TESTING: LAST WEEK (June 7-13, Test Volume June 3-9 )",
                    DatasetTimes("last", date(2021, 6, 13), date(2021, 6, 9))),
            example("VIRAL (RT-PCR) LAB TESTING: LAST WEEK (March 7-13)",
                    DatasetTimes("last", date(2021, 3, 13), date(2021, 3, 13)))
        ]
        for ex in examples:
            assert DatasetTimes.from_header(ex.given, date(2021, 12, 31)) == ex.expected, ex.given

        # test year boundary
        examples = [
            example("TESTING: LAST WEEK (October 24-30, Test Volume October 20-26)",
                    DatasetTimes("last", date(2020, 10, 30), date(2020, 10, 26))),
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

        test_df = geomapper.replace_geocode(
            pd.DataFrame({
                'geo_id': ['pa', 'wv'],
                'timestamp': [datetime(year=2020, month=1, day=1)]*2,
                'val': [15., 150.],}),
            "state_id",
            "state_code",
            "geo_id"
        )
        
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
                'val': [15. + 150.],}),
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
                'val': [15*pa_pop/tot_pop + 150*wv_pop/tot_pop],}),
            check_like=True
        )
