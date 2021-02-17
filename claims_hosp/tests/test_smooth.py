# third party
import numpy as np

# first party
from delphi_claims_hosp.smooth import left_gauss_linear


class TestLeftGaussSmoother:
    def test_gauss_linear(self):
        signal = np.ones(10)
        assert np.allclose(left_gauss_linear(signal)[1:], signal[1:])

        signal = np.arange(1, 10) + np.random.normal(0, 1, 9)
        assert np.allclose(left_gauss_linear(signal, 0.1)[1:], signal[1:])
