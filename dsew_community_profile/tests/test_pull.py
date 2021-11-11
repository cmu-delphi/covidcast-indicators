from collections import namedtuple
from datetime import date
import pandas as pd
import pytest

from delphi_dsew_community_profile.pull import DatasetTimes, as_reference_date
from delphi_dsew_community_profile.pull import Dataset

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

    def test_as_reference_date(self):
        examples = [
            example("TESTING: LAST WEEK (October 24-30, Test Volume October 20-26)",
                    DatasetTimes("last", date(2021, 10, 30), date(2021, 10, 26))),
            example("TESTING: PREVIOUS WEEK (October 24-30, Test Volume October 20-26)",
                    DatasetTimes("previous", date(2021, 10, 30), date(2021, 10, 26))),
            example("TESTING: LAST WEEK (October 24-November 30, Test Volume October 20-26)",
                    DatasetTimes("last", date(2021, 11, 30), date(2021, 10, 26))),
        ]
        for ex in examples:
            assert as_reference_date(ex.given) == ex.expected, ex.given

    def test_Dataset_skip_overheader(self):
        examples = [
            example("TESTING: LAST WEEK (October 24-30, Test Volume October 20-26)",
                    False),
            example("TESTING: PREVIOUS WEEK (October 17-23, Test Volume October 13-19)",
                    False),
            example("TESTING: % CHANGE FROM PREVIOUS WEEK",
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
                    False)
        ]
        for ex in examples:
            assert Dataset.retain_header(ex.given) == ex.expected, ex.given
            
    def test_Dataset_parse_sheet(self):
        # TODO
        pass
