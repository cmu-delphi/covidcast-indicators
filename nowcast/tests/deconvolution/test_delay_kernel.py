import numpy as np

from delphi_nowcast.deconvolution.delay_kernel import get_florida_delay_distribution


class TestDelayKernel:

    def test_get_florida_delay_distribution(self):
        delay_dist, coefs = get_florida_delay_distribution()
        assert np.all(np.array(delay_dist) < 1) and np.all(np.array(delay_dist) > 0)
        assert np.all(np.array(coefs) < 10) and np.all(np.array(coefs) >= 0)

        # loc param is fixed at 0
        assert coefs[1] == 0

        ## could add same test above with get_florida_delay_distribution(update=True)
        ## but runtime could be too long?
