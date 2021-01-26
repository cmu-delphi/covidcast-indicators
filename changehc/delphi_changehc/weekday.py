"""
Weekday effects (code from Aaron Rumack).

Created: 2020-05-06
"""

# third party
import cvxpy as cp
import numpy as np
from cvxpy.error import SolverError

# first party
from .config import Config


class Weekday:
    """Class to handle weekday effects."""

    @staticmethod
    def get_params(data):
        r"""Correct a signal estimated as numerator/denominator for weekday effects.

        The ordinary estimate would be numerator_t/denominator_t for each time point
        t. Instead, model

        log(numerator_t/denominator_t) = alpha_{wd(t)} + phi_t

        where alpha is a vector of fixed effects for each weekday. For
        identifiability, we constrain \sum alpha_j = 0, and to enforce this we set
        Sunday's fixed effect to be the negative sum of the other weekdays.

        We estimate this as a penalized Poisson GLM problem with log link. We
        rewrite the problem as

        log(numerator_t) = alpha_{wd(t)} + phi_t + log(denominator_t)

        and set a design matrix X with one row per time point. The first six columns
        of X are weekday indicators; the remaining columns are the identity matrix,
        so that each time point gets a unique phi. Using this X, we write

        log(numerator_t) = X beta + log(denominator_t)

        Hence the first six entries of beta correspond to alpha, and the remaining
        entries to phi.

        The penalty is on the L1 norm of third differences of phi (so the third
        differences of the corresponding columns of beta), to enforce smoothness.
        Third differences ensure smoothness without removing peaks or valleys.

        Objective function is negative mean Poisson log likelihood plus penalty:

        ll = (numerator * (X*b + log(denominator)) - sum(exp(X*b) + log(denominator)))
                / num_days

        Return a matrix of parameters: the entire vector of betas, for each time
        series column in the data.
        """
        tmp = data.reset_index()
        denoms = tmp.groupby(Config.DATE_COL).sum()["den"]
        nums = tmp.groupby(Config.DATE_COL).sum()["num"]
        n_nums = 1  # only one numerator column

        # Construct design matrix to have weekday indicator columns and then day
        # indicators.
        X = np.zeros((nums.shape[0], 6 + nums.shape[0]))  # pylint: disable=invalid-name
        not_sunday = np.where(nums.index.dayofweek != 6)[0]
        X[not_sunday, np.array(nums.index.dayofweek)[not_sunday]] = 1
        X[np.where(nums.index.dayofweek == 6)[0], :6] = -1
        X[:, 6:] = np.eye(X.shape[0])

        npnums, npdenoms = np.array(nums), np.array(denoms)
        params = np.zeros((n_nums, X.shape[1]))

        # fit model
        b = cp.Variable((X.shape[1]))
        lmbda = cp.Parameter(nonneg=True)
        lmbda.value = 10  # Hard-coded for now, seems robust to changes
        ll = (cp.matmul(npnums, cp.matmul(X, b) + np.log(npdenoms))
              - cp.sum(cp.exp(cp.matmul(X, b) + np.log(npdenoms)))
              ) / X.shape[0]
        penalty = (lmbda * cp.norm(cp.diff(b[6:], 3), 1) / (X.shape[0] - 2)
                   )  # L-1 Norm of third differences, rewards smoothness
        try:
            prob = cp.Problem(cp.Minimize(-ll + lmbda * penalty))
            _ = prob.solve()
        except SolverError:
            # If the magnitude of the objective function is too large, an error is
            # thrown; Rescale the objective function
            prob = cp.Problem(cp.Minimize((-ll + lmbda * penalty) / 1e5))
            _ = prob.solve()
        params = b.value

        return params

    @staticmethod
    def calc_adjustment(params, sub_data):
        """Apply the weekday adjustment to a specific time series.

        Extracts the weekday fixed effects from the parameters and uses these to
        adjust the time series.

        Since

        log(numerator_t / denominator_t) = alpha_{wd(t)} + phi_t,

        we have that

        numerator_t / denominator_t = exp(alpha_{wd(t)}) exp(phi_t)

        and can divide by exp(alpha_{wd(t)}) to get a weekday-corrected ratio. In
        this case, we only divide the numerator, leaving the denominator unchanged
        -- this has the same effect.

        """
        tmp = sub_data.reset_index()

        wd_correction = np.zeros((len(tmp["num"])))
        for wd in range(7):
            mask = tmp[Config.DATE_COL].dt.dayofweek == wd
            wd_correction[mask] = tmp.loc[mask, "num"] / (
                np.exp(params[wd]) if wd < 6 else np.exp(-np.sum(params[:6]))
            )
        tmp.loc[:, "num"] = wd_correction

        return tmp.set_index(Config.DATE_COL)
