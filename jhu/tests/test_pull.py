from os.path import join
import re
import pytest

from delphi_utils import GeoMapper
from delphi_jhu.pull import pull_jhu_data

class TestPullJHU:
    test_date_cols = ['2/29/20', '3/1/20', '3/2/20', '3/3/20', '3/4/20', '3/5/20', '3/6/20',
       '3/7/20', '3/8/20', '3/9/20', '3/10/20', '3/20', 'hamburger']

    def test_detect_date_col(self):
        is_date = [re.match(r'\d{1,2}\/\d{1,2}\/\d{1,2}', ds) for ds in self.test_date_cols]
        assert all(is_date[:-2])
        assert not any(is_date[-2:])

    def test_good_file(self):
        gmpr = GeoMapper()
        df = pull_jhu_data(join("test_data", "small_{metric}.csv"), "deaths", gmpr)

        assert (
            df.columns.values
            == ["fips", "timestamp", "new_counts", "cumulative_counts"]
        ).all()
        assert True

    def test_missing_days(self):
        gmpr = GeoMapper()
        with pytest.raises(ValueError):
            pull_jhu_data(
                join("test_data", "bad_{metric}_missing_days.csv"), "confirmed", gmpr
            )

    """Not sure if this is still relevant with using UID ...
    def test_missing_cols(self):

        with pytest.raises(ValueError):
            df = pull_jhu_data(
                join("test_data", "bad_{metric}_missing_cols.csv"), "confirmed", pop_df
            )

    def test_extra_cols(self):

        with pytest.raises(ValueError):
            df = pull_jhu_data(
                join("test_data", "bad_{metric}_extra_cols.csv"), "confirmed", pop_df
            )
    """
