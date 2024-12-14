import numpy as np

from delphi_doctor_visits.direction import (
    running_mean,
    running_sd,
    first_difference_direction,
)


class TestDirection:
    def test_running_mean(self):

        output = running_mean(np.array([1, 2, 3, 4]))
        assert (output == np.array([1, 1.5, 2, 2.5])).all()

    def test_running_sd(self):

        output = running_sd(np.array([1, 2, 3, 4]))
        assert np.max(np.abs(output ** 2 - np.array([0, 0.25, 2 / 3, 1.25]))) < 1e-8

    def test_first_difference_direction(self):

        output = first_difference_direction(np.array([1, 2, 3, 2, 3]))
        assert (output == np.array(["NA", "NA", "NA", "-1", "0"])).all()

        output = first_difference_direction(np.array([1, 2, 3, 4, 6]))
        assert (output == np.array(["NA", "NA", "NA", "0", "+1"])).all()
