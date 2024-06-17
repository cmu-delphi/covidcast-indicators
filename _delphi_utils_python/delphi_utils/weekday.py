"""
Weekday effects (code from Aaron Rumack).

Created: 2020-05-06
"""
import cvxpy as cp
import numpy as np
from cvxpy.error import SolverError


class Weekday:
    """Class to handle weekday effects."""

    @staticmethod
    def get_params(data, denominator_col, numerator_cols, date_col, scales, logger):
        r"""Fit weekday correction for each col in numerator_cols.

        Return a matrix of parameters: the entire vector of betas, for each time
        series column in the data.
        """
        tmp = data.reset_index()
        denoms = tmp.groupby(date_col).sum()[denominator_col]
        nums = tmp.groupby(date_col).sum()[numerator_cols]

        # Construct design matrix to have weekday indicator columns and then day
        # indicators.
        X = np.zeros((nums.shape[0], 6 + nums.shape[0]))
        not_sunday = np.where(nums.index.dayofweek != 6)[0]
        X[not_sunday, np.array(nums.index.dayofweek)[not_sunday]] = 1
        X[np.where(nums.index.dayofweek == 6)[0], :6] = -1
        X[:, 6:] = np.eye(X.shape[0])

        npnums, npdenoms = np.array(nums), np.array(denoms)
        params = np.zeros((nums.shape[1], X.shape[1]))

        # Loop over the available numerator columns and smooth each separately.
        for i in range(nums.shape[1]):
            result = Weekday._fit(X, scales, npnums[:, i], npdenoms)
            if result is None:
                logger.error("Unable to calculate weekday correction")
            else:
                params[i,:] = result

        return params

    @staticmethod
    def _fit(X, scales, npnums, npdenoms):
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
        """
        b = cp.Variable((X.shape[1]))

        lmbda = cp.Parameter(nonneg=True)
        lmbda.value = 10  # Hard-coded for now, seems robust to changes

        ll = (
            cp.matmul(npnums, cp.matmul(X, b) + np.log(npdenoms)) -
            cp.sum(cp.exp(cp.matmul(X, b) + np.log(npdenoms)))
        ) / X.shape[0]
        # L-1 Norm of third differences, rewards smoothness
        penalty = lmbda * cp.norm(cp.diff(b[6:], 3), 1) / (X.shape[0] - 2)
        for scale in scales:
            try:
                prob = cp.Problem(cp.Minimize((-ll + lmbda * penalty) / scale))
                _ = prob.solve()
                return b.value
            except SolverError:
                # If the magnitude of the objective function is too large, an error is
                # thrown; Rescale the objective function by going through loop
                continue
        return None

    @staticmethod
    def calc_adjustment(params, sub_data, cols, date_col):
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
        tmp = sub_data.copy()

        for i, c in enumerate(cols):
            wd_correction = np.zeros((len(tmp[c])))

            for wd in range(7):
                mask = tmp[date_col].dt.dayofweek == wd
                wd_correction[mask] = tmp.loc[mask, c] / (
                    np.exp(params[i, wd]) if wd < 6 else np.exp(-np.sum(params[i, :6]))
                )
            tmp.loc[:, c] = wd_correction
        return tmp
