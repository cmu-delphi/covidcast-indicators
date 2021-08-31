import pytest

from delphi_sir_complainsalot.check_source import _is_retired


class TestCheckSource:

    @pytest.mark.parametrize(
        "row, expected",
        [
            ({"signal": "confirmed_7dav_cumulative_num", "geo_type": "county"}, True),
            ({"signal": "confirmed_7dav_cumulative_num", "geo_type": "msa"}, True),
            ({"signal": "confirmed_7dav_cumulative_num", "geo_type": "nation"}, False),
            ({"signal": "confirmed_7dav_cumulative_prop", "geo_type": "nation"}, True),
            ({"signal": "confirmed_7dav_cumulative_prop", "geo_type": "state"}, True),
            ({"signal": "confirmed_7dav_cumulative_prop", "geo_type": "some_other_geo"}, True),
            ({"signal": "deaths_7dav_cumulative_num", "geo_type": "state"}, True),
            ({"signal": "deaths_7dav_cumulative_num", "geo_type": "county"}, False),
            ({"signal": "deaths_7dav_cumulative_prop", "geo_type": "nation"}, True),
            ({"signal": "deaths_7dav_cumulative_prop", "geo_type": "hhs"}, False),
        ]
    )
    def test__is_retired(self, row, expected):
        retired_signals = [
            ["confirmed_7dav_cumulative_num", "county", "msa"],
            "confirmed_7dav_cumulative_prop",
            ["deaths_7dav_cumulative_num", "state"],
            ["deaths_7dav_cumulative_prop", "nation"]
        ]
        assert _is_retired(row, retired_signals) == expected
        assert _is_retired(row, None) == False
