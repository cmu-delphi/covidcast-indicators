from datetime import date
from unittest.mock import patch

import numpy as np

from delphi_nowcast.deconvolution.ground_truth import fill_missing_days, construct_truths
from delphi_nowcast.data_containers import LocationSeries, SensorConfig


class TestConstructTruths:

    @patch("delphi_nowcast.deconvolution.ground_truth.deconvolve_double_smooth_tf_cv")
    @patch("delphi_nowcast.deconvolution.ground_truth.get_historical_sensor_data")
    @patch("delphi_nowcast.deconvolution.ground_truth.get_indicator_data")
    def test_construct_truths(self, mock_indicators, mock_history, mock_deconvolve):
        mock_indicators.return_value = {
            ("src", "sig", "state", "nv"):
                LocationSeries("nv", "state", {date(2020, 1, 1): 0, date(2020, 1, 2): 0, date(2020, 1, 3): 0})
                # these values are not used, only the dates to determine which missing dates can be filled
        }
        mock_history.side_effect = [(LocationSeries("nv",
                                                    "state",
                                                    {date(2020, 1, 1): 1.5,
                                                     date(2020, 1, 3): 3.5,
                                                     date(2020, 1, 5): 5.5}),
                                     [date(2020, 1, 2), date(2020, 1, 4)]),
                                    (LocationSeries("ca",
                                                    "state",
                                                    {date(2020, 1, 1): 1,
                                                     date(2020, 1, 3): 3,
                                                     date(2020, 1, 5): 5}),
                                     [date(2020, 1, 2), date(2020, 1, 4)])
                                    ]
        mock_deconvolve.return_value = [1, 2, 3, 2.5]
        test_config = SensorConfig("src", "sig", "", 0)
        test_locations = [LocationSeries("nv", "state"), LocationSeries("ca", "state")]
        test_output = construct_truths(date(2020, 1, 1),
                                       date(2020, 1, 5),
                                       date(2020, 1, 5),
                                       test_config,
                                       test_locations)
        assert test_output == {test_config: [
                                   LocationSeries("nv",
                                                  "state",
                                                  {date(2020, 1, 1): 1.5,
                                                   date(2020, 1, 2): 2.5,
                                                   date(2020, 1, 3): 3.5,
                                                   date(2020, 1, 5): 5.5}),
                                   LocationSeries("ca",
                                                  "state",
                                                  {date(2020, 1, 1): 1,
                                                   date(2020, 1, 3): 3,
                                                   date(2020, 1, 5): 5})
                               ]}


class TestFillMissingDays:

    @patch("delphi_nowcast.deconvolution.ground_truth.deconvolve_double_smooth_tf_cv")
    def test_fill_missing_days(self, mock_deconvolve):
        mock_deconvolve.return_value = [1, 2, 3, 2.5]
        test_input = LocationSeries("",
                                    "",
                                    {date(2020, 1, 1): 1.5,
                                     date(2020, 1, 3): 3.5,
                                     date(2020, 1, 5): 5.5})
        indicator_data = LocationSeries("",
                                        "",
                                        {date(2020, 1, 1): 1,
                                         date(2020, 1, 2): 2,
                                         date(2020, 1, 3): 3,
                                         date(2020, 1, 3): 4})
        missing_dates = [date(2020, 1, 2), date(2020, 1, 4)]
        test_output, test_export = fill_missing_days(test_input, indicator_data, missing_dates)
        # should skip 1/4 since no indicator data for that day
        assert test_output == LocationSeries("",
                                             "",
                                             {date(2020, 1, 1): 1.5,
                                              date(2020, 1, 2): 2.5,
                                              date(2020, 1, 3): 3.5,
                                              date(2020, 1, 5): 5.5})
        assert test_export == LocationSeries("",
                                             "",
                                             {date(2020, 1, 2): 2.5})

    def test_no_missing(self):
        test_input = LocationSeries("",
                                    "",
                                    {date(2020, 1, 1): 1.5,
                                     date(2020, 1, 3): 3.5,
                                     date(2020, 1, 5): 5.5})
        indicator_data = LocationSeries("", "")
        missing_dates = []
        test_output, test_export = fill_missing_days(test_input, indicator_data, missing_dates)
        assert test_output == test_input
        assert test_export == LocationSeries("", "")
