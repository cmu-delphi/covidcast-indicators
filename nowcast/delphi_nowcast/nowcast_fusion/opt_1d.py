"""
====================================================================
THIS CODE IS COPIED FROM
https://github.com/cmu-delphi/nowcast/blob/main/src/fusion/opt_1d.py
====================================================================

Provides derivative-free optimization over a bounded, one-dimensional interval.

The function to optimize doesn't have to be convex, but it is assumed that it
has a single maximum and is monotonically decreasing away from that maximum in
both directions.

More general optimization problems can be solved using, for example, the
Nelder-Mead algorithm.

See also: neldermead.py
"""


def maximize(low, high, objective, stop):
  """
  Find the scalar argument which maximizes the objective function. The search
  space is bounded to the closed interval [low, high].

  input:
    low: the lower bound of the search interval
    high: the upper bound of the search interval
    objective: an objective function, which takes and returns a scalar
    stop: a function which returns whether the search should be stopped, given
        the following parameters:
      - number of times the objective function has been called
      - width of the current search interval
      - the maximum value of the objective function so far

  output:
    a tuple consisting of:
      - the argument which maximizes the objective function
      - the maximum value of the objective function
  """

  # The algorithm below is inspired by the Nelder-Mead and bisection methods.

  # This method tracks a set of four points and their associated values, as
  # returned by the objective function. One of the values must be less than or
  # equal to the remaining values. Its point -- the argmin -- is iteratively
  # updated. If the argmax is not on the boundary, then the argmin is updated
  # to bisect the two argmax points. Otherwise, the two argmin points are
  # updated to trisect the two argmax points. Iteration continues until the
  # stop function returns truth.

  diff = high - low
  a, b, c, d = low, low + 1 / 3 * diff, low + 2 / 3 * diff, high
  w, x, y, z = [objective(i) for i in (a, b, c, d)]
  argmax = lambda: max(enumerate([w, x, y, z]), key=lambda k: k[1])[0]
  n = 4
  i = argmax()
  while not stop(n, d - a, [w, x, y, z][i]):
    if i == 0:
      diff = b - a
      b, c, d = a + 1 / 3 * diff, a + 2 / 3 * diff, b
      x, y, z = objective(b), objective(c), x
      n += 2
    elif i == 3:
      diff = d - c
      a, b, c = c, c + 1 / 3 * diff, c + 2 / 3 * diff
      w, x, y = y, objective(b), objective(c)
      n += 2
    elif i == 1:
      if c - b > b - a:
        c, d = (b + c) / 2, c
        y, z = objective(c), y
      else:
        b, c, d = (a + b) / 2, b, c
        x, y, z = objective(b), x, y
      n += 1
    else:
      if d - c > c - b:
        a, b, c = b, c, (c + d) / 2
        w, x, y = x, y, objective(c)
      else:
        a, b = b, (b + c) / 2
        w, x = x, objective(b)
      n += 1
    i = argmax()
  return ([a, b, c, d][i], [w, x, y, z][i])
