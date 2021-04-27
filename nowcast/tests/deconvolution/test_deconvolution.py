import numpy as np
import pytest


from delphi_nowcast.deconvolution.deconvolution import (deconvolve_double_smooth_ntf, deconvolve_double_smooth_tf_cv,
                                                        _linear_extrapolate, _construct_poly_interp_mat,
                                                        _impute_with_neighbors, _fft_convolve, _soft_thresh)

class TestDeconvolveDoubleSmoothTFCV:

    def test_deconvolve_double_smooth_tf_cv(self):
        np.testing.assert_allclose(
            deconvolve_double_smooth_tf_cv(np.arange(20), np.arange(20), np.array([0,1])),
            np.arange(1,21).astype(float)
        )

class TestDeconvolveDoubleSmoothNTF:

    def test_deconvolve_double_smooth_ntf(self):
        np.testing.assert_allclose(
            deconvolve_double_smooth_ntf(np.arange(20), np.arange(20), np.array([0,1]), lam=1, gam=0),
            np.arange(1,21).astype(float)
        )

    def test_deconvolve_double_smooth_ntf_infgamma(self):
        # check large gamme means last values are the same
        deconv_vals = deconvolve_double_smooth_ntf(np.arange(20), np.arange(20), np.array([0,1]), lam=1, gam=1e10)
        assert np.isclose(deconv_vals[-1], deconv_vals[-2])


class Test_SoftThresh:

    def test__soft_thresh(self):
        np.testing.assert_array_equal(
            _soft_thresh(np.arange(-3,4), 1),
            np.array([-2, -1,  0,  0,  0,  1,  2])
        )

class Test_FFTConvolve:

    def test__fft_convolve(self):
        np.testing.assert_array_equal(
            _fft_convolve(np.array([1, 0, 1]), np.array([2, 7])),
            np.array([2, 7, 2])
        )

class Test_ImputeWithNeighbors:

    def test__impute_with_neighbors(self):
        np.testing.assert_array_equal(
            _impute_with_neighbors([np.nan, 1, np.nan, 3, np.nan]),
            np.array([1, 1, 2, 3, 3])
        )


    def test__impute_with_neighbors_no_missing(self):
        np.testing.assert_array_equal(
            _impute_with_neighbors(np.arange(5)),
            np.arange(5)
        )

class Test_ConstructPolyInterpMat:

    def test__construct_poly_interp_mat(self):
        np.testing.assert_array_equal(
            _construct_poly_interp_mat(np.arange(6), 3),
            np.array([[3., -2.],
                      [2., -1.],
                      [1., 0.],
                      [0., 1.],
                      [-1., 2.],
                      [-2., 3.]])
        )

    def test__construct_poly_interp_mat_wrong_k(self):
        with pytest.raises(AssertionError):
            _construct_poly_interp_mat(np.arange(6), 2)


class Test_LinearExtrapolate:

    def test__linear_extrapolate(self):
        assert _linear_extrapolate(0, 0, 1, 3, 4) == 12
