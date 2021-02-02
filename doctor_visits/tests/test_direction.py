import pytest

import numpy as np

from delphi_doctor_visits.direction import (
    running_mean,
    running_sd,
    first_difference_direction,
)


class TestDirection:
    def test_running_mean(self):

        input = np.array([1, 2, 3, 4])
        output = running_mean(input)
        assert (output == np.array([1, 1.5, 2, 2.5])).all()

    def test_running_sd(self):

        input = np.array([1, 2, 3, 4])
        output = running_sd(input)
        assert np.max(np.abs(output ** 2 - np.array([0, 0.25, 2 / 3, 1.25]))) < 1e-8

    def test_first_difference_direction(self):

        input = np.array([1, 2, 3, 2, 3])
        output = first_difference_direction(input)
        assert (output == np.array(["NA", "NA", "NA", "-1", "0"])).all()

        input = np.array([1, 2, 3, 4, 6])
        output = first_difference_direction(input)
        assert (output == np.array(["NA", "NA", "NA", "0", "+1"])).all()
