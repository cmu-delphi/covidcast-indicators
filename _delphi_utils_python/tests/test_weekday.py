import logging

import numpy as np
import pandas as pd
from delphi_utils.weekday import Weekday


class TestWeekday:

    TEST_DATA = pd.DataFrame({
        "num": np.arange(1, 11, 1),
        "den": np.arange(11, 21, 1),
        "date": pd.date_range("2020-01-01", "2020-01-10")
    })

    def test_get_params(self):
        TEST_LOGGER = logging.getLogger()

        result = Weekday.get_params(self.TEST_DATA, "den", ["num"], "date", [1], TEST_LOGGER)
        print(result)
        expected_result = np.array([[-0.05998306, -0.07269935, -0.05603804,  0.03437098,  0.12530953,
                0.04554737, -2.27674403, -1.89568887, -1.56957556, -1.29840412,
                -1.08217453, -0.9208868 , -0.81454092, -0.76313691, -0.76667475,
                -0.82515445]])
        assert np.allclose(result, expected_result)

    def test_calc_adjustment_with_zero_parameters(self):
        params = np.array([[0, 0, 0, 0, 0, 0, 0]])

        result = Weekday.calc_adjustment(params, self.TEST_DATA, ["num"], "date")

        # Data should be unchanged when params are 0's
        assert np.allclose(result["num"].values, self.TEST_DATA["num"].values)
        assert np.allclose(result["den"].values, self.TEST_DATA["den"].values)
        assert np.array_equal(result["date"].values, self.TEST_DATA["date"].values)

    def test_calc_adjustment(self):
        params = np.array([[1, -1, 1, -1, 1, -1, 1]])

        result = Weekday.calc_adjustment(params, self.TEST_DATA, ["num"], "date")

        print(result["num"].values)
        print(result["den"].values)
        expected_nums = [
            0.36787944,
            5.43656366,
            1.10363832,
            10.87312731,
            5,
            2.20727665,
            19.0279728,
            2.94303553,
            24.46453646,
            3.67879441,
        ]

        # The date and "den" column are unchanged by this function
        assert np.allclose(result["num"].values, expected_nums)
        assert np.allclose(result["den"].values, self.TEST_DATA["den"].values)
        assert np.array_equal(result["date"].values, self.TEST_DATA["date"].values)
