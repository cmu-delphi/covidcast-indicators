"""
========================================================================
THIS CODE IS COPIED FROM
https://github.com/cmu-delphi/nowcast/blob/main/src/fusion/covariance.py
========================================================================

Maximum likelihood covariance estimation that is robust to insufficient and
missing values.
"""

# standard library
import abc

# third party
import numpy as np
import scipy.linalg
import scipy.stats

# first party
from .opt_1d import maximize


def nancov(X):
  """
  Estimate the covariance matrix of partially observed data, ignoring nans.
  The covariance matrix is the elementwise quotient of the returned numerator
  and denominator matrices. Data columns are assumed to be unbiased.

  Denominator elements may be zero, leading to undefined covariance. Further,
  the resulting matrix may have nonpositive eigenvalues. As a result, it may
  not be invertable or positive definite.

  input:
    X: data matrix (N x P) (N observations, P variables)

  output:
    numerator (P x P), denominator (P x P)
  """

  # a helper function which computes the dot of a matrix with itself
  tdot = lambda M: np.dot(M.T, M)

  # The numerator is the dot product of each column, where nans are replaced
  # with zeros. The denominator is the dot product of each column, where nans
  # are replaced with zeros and everything else is replaced with ones.
  return tdot(np.nan_to_num(X)), tdot(np.isfinite(X).astype(np.float))


def log_likelihood(cov, data):
  """
  Return the log-likelihood of data, given parameters. The mean is assumed to
  be zero, or a vector of zeros, as appropriate.

  input:
    cov: covariance matrix (P x P) (P variables)
    data: data matrix (N x P) (N observations)

  output:
    log-likelihood in the range (-np.inf, 0)
  """
  mean = np.zeros(cov.shape[0])
  try:
    # Attempt to compute the log likelihood. This will fail with `ValueError`
    # if the covariance matrix is not positive semidefinite. Otherwise, this
    # will fail with `LinAlgError` if the covariance matrix is near-singular.
    return np.sum(scipy.stats.multivariate_normal.logpdf(data, mean, cov=cov))
  except (ValueError, np.linalg.LinAlgError):
    # Return log likelihood of negative infinity when the covariance matrix is
    # not firmly positive definite.
    return -np.inf


class ShrinkageMethod(metaclass=abc.ABCMeta):
  """
  An abstract class representing a method for shrinking a covariance matrix.
  This may be necessary, for example, when there are missing values or too few
  observations. The goal is to find the positive definite matrix which
  maximizes the multivariate normal likelihood of the available data.
  """

  @abc.abstractmethod
  def get_alpha_bounds(self):
    raise NotImplementedError()

  @abc.abstractmethod
  def get_cov(self, alpha):
    raise NotImplementedError()


class DenominatorModifier(ShrinkageMethod):
  """
  An abstract subclass of ShrinkageMethod representing methods that operate by
  modifying the offdiagonal entries of the denominator of the empirical
  covariance matrix.
  """

  def __init__(self, cov_num, cov_den, num_obs):
    self.offdiag = np.ones(cov_den.shape) - np.eye(cov_den.shape[0])
    self.cov_num = cov_num
    self.cov_den = cov_den
    self.cov_den_diag = cov_den * (1 - self.offdiag)
    self.cov_den_offdiag = cov_den * self.offdiag
    n = cov_num.shape[0]
    self.num_obs = num_obs
    self.needed_obs = max(num_obs, (n + 1) * n / 2)


class BlendDiagonal0(DenominatorModifier):
  """Multiply the offdiagonal entries of the denominator by a constant."""

  def __init__(self, cov_num, cov_den, num_obs):
    super().__init__(cov_num, np.maximum(cov_den, 1), num_obs)

  def get_alpha_bounds(self):
    return [1, self.needed_obs]

  def get_cov(self, alpha):
    return self.cov_num / (self.cov_den_diag + self.cov_den_offdiag * alpha)


class BlendDiagonal1(DenominatorModifier):
  """Add a constant to the offdiagonal entries of the denominator."""

  def __init__(self, cov_num, cov_den, num_obs):
    super().__init__(cov_num, cov_den, num_obs)

  def get_alpha_bounds(self):
    low = 0 if np.min(self.cov_den) > 0 else 1
    return [low, self.needed_obs]

  def get_cov(self, alpha):
    return self.cov_num / (self.cov_den + self.offdiag * alpha)


class BlendDiagonal2(DenominatorModifier):
  """Blend offdiagonal entries of the denominator with N."""

  def __init__(self, cov_num, cov_den, num_obs):
    super().__init__(cov_num, cov_den, num_obs)

  def get_alpha_bounds(self):
    low = 0 if np.min(self.cov_den) > 0 else 1
    return [low, self.needed_obs]

  def get_cov(self, alpha):
    a = alpha / self.needed_obs
    x, y = self.cov_den_offdiag, self.offdiag * self.needed_obs
    return self.cov_num / (self.cov_den_diag + (1 - a) * x + a * y)


def posdef_max_likelihood_objective(X, shrinkage):
  """
  Return an objective function with which to find an optimal shrinkage value.
  Optimal is defined as the value which maximizes the likelihood of the
  shrunk covariance, given the data. If the shrunk covariance matrix is not
  positive definite, then the objective function returns negative infinity.

  input:
    X: data matrix (N x P) (N observations, P variables)
    shrinkage: an instance of absract class ShrinkageMethod

  output:
    an objective function suitable the mle_cov function
  """

  # replace missing values (nans) with zeros
  X0 = np.nan_to_num(X)

  # define an objective function, given the data
  objective = lambda alpha: log_likelihood(shrinkage.get_cov(alpha), X0)

  # return the objective function
  return objective


def mle_cov(X, shrinkage_class):
  """
  Find the covariance matrix that maximizes the likelihood of a multivariate
  normal disribution, given observed data. It is assumed that the data is
  already unbiased. The data may have mising values and may not have a
  sufficient number of observations to uniquely determine the covariance
  matrix. The returned covariance matrix is guaranteed to be positive definite,
  making it suitable for applications (for example, sensorization fusion) which
  require a precision matrix.

  input:
    X: data matrix (N x P) (N observations, P variables)
    shrinkage_class: a concrete subclass of ShrinkageMethod

  output:
    the shrunk covariance matrix with maximum likelihood (P x P)
  """

  # sanity check
  if X.shape[0] < 2:
    raise Exception('need at least two observations to estimate covariance')

  # get the numerator and denominator of the empirical covariance matrix
  cov_num, cov_den = nancov(X)

  # instantiate the shrinkage method
  shrinkage = shrinkage_class(cov_num, cov_den, X.shape[0])

  # obtain an objective function
  low, high = shrinkage.get_alpha_bounds()
  objective = posdef_max_likelihood_objective(X, shrinkage)
  stop = lambda n_obj, d_alpha, max_ll: d_alpha <= 1

  # let the optimizer find a good shrinkage parameter
  alpha, ll = maximize(low, high, objective, stop)

  # return the shrunk covariance matrix with maximum likelihood
  return shrinkage.get_cov(alpha)
