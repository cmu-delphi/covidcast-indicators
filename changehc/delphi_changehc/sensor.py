"""
Sensor class to fit a signal using Covid counts from Change HC outpatient data.

Author: Aaron Rumack
Created: 2020-10-14

"""

# standard packages
import logging

# third party
import numpy as np
import pandas as pd
from delphi_utils import Smoother

# first party
from .config import Config



class CHCSensor:
    """Sensor class to fit a signal using Covid counts from Change HC outpatient data."""

    smoother = Smoother("savgol",
                        poly_fit_degree=1,
                        gaussian_bandwidth=Config.SMOOTHER_BANDWIDTH)

    @staticmethod
    def gauss_smooth(count,total):
        """Smooth using the left_gauss_linear.

        Args:
            count, total: array
        """
        count_smooth = CHCSensor.smoother.smooth(count)
        total_smooth = CHCSensor.smoother.smooth(total)
        total_clip = np.clip(total_smooth, 0, None)
        count_clip = np.clip(count_smooth, 0, total_clip)
        return count_clip, total_clip

    @staticmethod
    def backfill(
            num,
            den,
            k=Config.MAX_BACKFILL_WINDOW,
            min_visits_to_fill=Config.MIN_CUM_VISITS):
        """
        Adjust for retroactively added observations (backfill) by using a variable length smoother.

        The smoother starts from the RHS and moves leftwards (backwards through time).
        We cumulatively sum the total visits (denominator), until we have observed some minimum number of
        counts, then calculate the sum over that bin. We restrict the
        bin size so to avoid including long-past values.

        Args:
            num: array of covid counts
            den: array of total visits
            k: maximum number of days used to average a backfill correction
            min_visits_to_fill: minimum number of total visits needed in order to sum a bin

        Returns: dataframes of adjusted covid counts, adjusted visit counts, inclusion array
        """
        if isinstance(den,(pd.DataFrame,pd.Series)):
            den = den.values
        if isinstance(num,(pd.DataFrame,pd.Series)):
            num = num.values
        revden = den[::-1]
        revnum = num[::-1].reshape(-1, 1)
        new_num = np.full_like(revnum, np.nan, dtype=float)
        new_den = np.full_like(revden, np.nan, dtype=float)
        n, p = revnum.shape

        for i in range(n):
            visit_cumsum = revden[i:].cumsum()

            # calculate backfill window
            closest_fill_day = np.where(visit_cumsum >= min_visits_to_fill)[0]
            if len(closest_fill_day) > 0:
                closest_fill_day = min(k, closest_fill_day[0])
            else:
                closest_fill_day = k

            if closest_fill_day == 0:
                new_den[i] = revden[i]

                for j in range(p):
                    new_num[i, j] = revnum[i, j]
            else:
                den_bin = revden[i: (i + closest_fill_day + 1)]
                new_den[i] = den_bin.sum()

                for j in range(p):
                    num_bin = revnum[i: (i + closest_fill_day + 1), j]
                    new_num[i, j] = num_bin.sum()

        new_num = new_num[::-1]
        new_den = new_den[::-1]

        return new_num, new_den

    @staticmethod
    def fit(y_data, first_sensor_date, geo_id, num_col="num", den_col="den"):
        """Fitting routine.

        Args:
            y_data: dataframe for one geo_id, indexed by date
            first_sensor_date: datetime of first date
            geo_id: unique identifier for the location column
            num_col: str name of numerator column
            den_col: str name of denominator column

        Returns:
            dictionary of results

        """
        # backfill
        total_counts, total_visits = CHCSensor.backfill(y_data[num_col].values,
                                                        y_data[den_col].values)

        # calculate smoothed counts and jeffreys rate
        # the left_gauss_linear smoother is not guaranteed to return values greater than 0

        smoothed_total_counts, smoothed_total_visits = CHCSensor.gauss_smooth(
            total_counts.flatten(), total_visits
        )

        # in smoothing, the numerator may have become more than the denominator
        # simple fix is to clip the max values elementwise to the denominator (note that
        # this has only been observed in synthetic data)
        # smoothed_total_counts = np.clip(smoothed_total_counts, 0, smoothed_total_visits)

        smoothed_total_rates = (
                (smoothed_total_counts + 0.5) / (smoothed_total_visits + 1)
        )

        # checks - due to the smoother, the first value will be NA
        assert (
                np.sum(np.isnan(smoothed_total_rates[1:])) == 0
        ), "NAs in rate calculation"
        assert (
                np.sum(smoothed_total_rates[1:] <= 0) == 0
        ), f"0 or negative value, {geo_id}"

        # cut off at sensor indexes
        rate_data = pd.DataFrame({'rate':smoothed_total_rates, 'den': smoothed_total_visits},
                                 index=y_data.index)
        rate_data = rate_data[first_sensor_date:]
        include = rate_data['den'] >= Config.MIN_DEN
        valid_rates = rate_data[include]
        se_valid = valid_rates.eval('sqrt(rate * (1 - rate) / den)')
        rate_data['se'] = se_valid

        logging.debug("{0}: {1:.3f},[{2:.3f}]".format(
            geo_id, rate_data['rate'][-1], rate_data['se'][-1]
        ))
        return {"geo_id": geo_id,
                "rate": 100 * rate_data['rate'],
                "se": 100 * rate_data['se'],
                "incl": include}
