from collections import namedtuple
from datetime import date
import pandas as pd
import pytest

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
            
    def test_Dataset_parse_sheet(self):
        # TODO
        pass
