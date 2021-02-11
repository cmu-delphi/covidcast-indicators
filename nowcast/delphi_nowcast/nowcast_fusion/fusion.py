"""
====================================================================
THIS CODE IS COPIED FROM
https://github.com/cmu-delphi/nowcast/blob/main/src/fusion/fusion.py
====================================================================

An implementation of the sensorization nowcast_fusion kernel and supporting methods. All
inputs and outputs are assumed to be of type numpy.ndarray.

See also:
  Farrow DC. "Modeling the Past, Present, and Future of Influenza" (Doctoral
  dissertation). 2016.
"""

# standard library
from fractions import Fraction

# third party
import numpy as np


def fuse(z, R, H):
  """
  Fuse measurement distribution into state distribution, given a linear mapping
  from state space to measurement space.

  input:
    z: row vector of sensorization measurements (1 x I)
    R: sensorization noise covariance matrix (I x I)
    H: matrix mapping from state space to measurement space (I x S)

  output:
    - the mean of the system state distribution (1 x S)
    - the covariance of the system state distribution (S x S)
  """

  # precompute common product
  RiH = np.dot(np.linalg.inv(R), H)

  # return the system state distribution
  P = np.linalg.inv(np.dot(H.T, RiH))
  x = np.dot(np.dot(z, RiH), P)
  return (x, P)


def extract(x, P, W):
  """
  Extract output distribution from state distribution, given a linear mapping
  from state space to output space.

  The diagonal elements of the output covariance matrix are the variance of
  each output variable.

  input:
    x: row vector of state mean (1 x S)
    P: state covariance matrix (S x S)
    W: matrix mapping from state space to output space (O x S)

  output:
    - the mean of the output distribution (1 x O)
    - the covariance of the output distribution (O x O)
  """

  # return the output distribution
  S = np.dot(np.dot(W, P), W.T)
  y = np.dot(x, W.T)
  return (y, S)


def eliminate(X):
  """
  Compute the canonical reduced row echelon form of the given matrix. The
  Gauss-Jordan algorithm is used to compute the elimination. The matrix is
  modified in-place.

  For numerical stability, it is strongly suggested that the elements of the
  input matrix be Fractions. Although discouraged, matrices of floats are also
  supported.

  input:
    X: the input matrix

  output:
    the matrix in reduced row echelon form
  """

  # dimensions
  num_r, num_c = X.shape

  # forward elimination
  r, c = 0, 0
  while r < num_r and c < num_c:
    values = [float(x) for x in X[r:, c]]
    i = r + np.argmax(np.abs(values))
    if X[i, c] != 0:
      if i != r:
        temp = X[i, :].copy()
        X[i, :] = X[r, :]
        X[r, :] = temp
      X[r, c:] /= X[r, c]
      for i in range(r + 1, num_r):
        X[i, c:] -= X[i, c] * X[r, c:]
      r += 1
    c += 1

  # backward substitution
  for r in range(num_r - 1, -1, -1):
    for c in range(num_c):
      if X[r, c] != 0:
        for i in range(r - 1, -1, -1):
          X[i, c:] -= X[i, c] * X[r, c:]
        break

  # return the result
  return X


def matmul(*matrices):
  """
  Compute the product of the given matrices. The matrices must all have
  elements of type Fraction or float. The type of the output will be the same
  as the type of the input.

  This function is not particularly efficient -- O(n^3) -- and is intended only
  for computing the product of matrices of fractions. The product of matrices
  of floats can be computed more efficiently by numpy or scipy.

  input:
    *matrices: the input matrices

  output:
    the product of inputs matrices
  """

  if len(matrices) == 1:
    return matrices[0]
  elif len(matrices) == 2:
    A, B = matrices
    (rows, size), (temp, cols) = A.shape, B.shape
    if size != temp:
      raise Exception('matrix dimensions do not match')
    dot = lambda U, V: sum(u * v for (u, v) in zip(U, V))
    vals = [[dot(A[r, :], B[:, c]) for c in range(cols)] for r in range(rows)]
    return np.array(vals)
  else:
    return matmul(matrices[0], matmul(*matrices[1:]))


def determine_statespace(H0, W0):
  """
  Return matrices mapping from latent statespace to input space and output
  space. These are the matrices H and W, respectively, used in the sensorization
  nowcast_fusion kernel. Since some outputs may be indeterminate, the indices of the
  fully determined rows are returned. This may be used, for example, to find
  the set of outputs which make up the rows of the returned W matrix.

  inputs:
    H0: map from full statespace to inputs (I x S)
    W0: map from full statespace to outputs (O x S)

  outputs:
    - the matrix H, mapping subspace to inputs (I x S')
    - the matrix W, mapping subspace to outputs (O' x S')
    - list of row indices of W0 that make up W (O')

  notes:
    - S' <= S and O' <= O
    - for numerical stability, inputs should be matrices of Fractions
  """

  # helper function to convert a float matrix into a fraction matrix
  fractions = lambda X: np.array([[Fraction(x) for x in row] for row in X])

  # Find a set of basis vectors that span the same subspace (of the full
  # statespace) that is spanned by the input vectors in H0. The result is a
  # minimal set of elements from which all inputs can be unambiguously
  # determined.
  B = eliminate(H0.copy())

  # the dimensions of full statespace (number of columns)
  size = B.shape[1]

  # the dimensions of the subspace (number of non-empty rows)
  rank = np.sum(np.sum(np.abs(B), axis=1) > 0)

  # B should be a square matrix with rows of zeros below rows of basis vectors
  num_rows = B.shape[0]
  if num_rows < size:
    Z = fractions(np.zeros((size - num_rows, size)))
    B = np.vstack((B, Z))
  elif num_rows > size:
    B = B[:size, :]

  # Attempt to build each input and output vector as a linear combination of
  # the subspace basis vectors. Since B may not be full rank, it may not be
  # invertible. Instead, solve by eliminating the augmented matrix of B
  # (transposed) with the identity matrix. After elimination, the (transposed)
  # inverse of B is contained within the augmented matrix.
  I = fractions(np.eye(size))
  BtI = np.hstack((B.T, I))
  IBit = eliminate(BtI)
  Bi = IBit[:, size:].T

  # possible, or "actual", solutions are in the leftmost columns
  # impossible, or "pseudo", solutions are in the rightmost columns
  Bi_actual, Bi_pseudo = Bi[:, :rank], Bi[:, rank:]

  # compute H, the map from statespace B to inputs
  # all inputs are within the span of statespace B
  H = matmul(H0, Bi_actual)

  # compute W, the map from statespace B to outputs
  # outputs not within the span of statespace B must be excluded
  W_actual = matmul(W0, Bi_actual)
  W_pseudo = matmul(W0, Bi_pseudo)

  # only keep rows where the coeficient of all pseudo basis vectors is zero
  actual_rows = np.flatnonzero(np.sum(np.abs(W_pseudo), axis=1) == 0)
  W = W_actual[actual_rows, :]

  # return H, W, and the indices of the rows of W0 that make up W
  return H, W, actual_rows
