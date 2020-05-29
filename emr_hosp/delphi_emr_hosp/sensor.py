"""
Sensor class to fit a signal using CLI counts from EMR Hospitalization data.

Author: Maria Jahja
Created: 2020-06-01

"""

# standard packages
import logging

# third party
import numpy as np
import pandas as pd

# first party
from .config import Config
from .smooth import left_gauss_linear


class EMRHospSensor:
    """Sensor class to fit a signal using CLI counts from EMR Hospitalization data.
    """

    @staticmethod
    def backfill(
        num,
        den,
        k=Config.MAX_BACKFILL_WINDOW,
        min_visits_to_fill=Config.MIN_CUM_VISITS):
        """
        Adjust for backfill (retroactively added observations) by using a
         variable length smoother, which starts from the RHS and moves
         leftwards (backwards through time). We cumulatively sum the total
         visits (denominator), until we have observed some minimum number of
         counts, then calculate the sum over that bin. We restrict the
         bin size so to avoid inluding long-past values.

        Args:
            num: dataframe of covid counts
            den: dataframe of total visits
            k: maximum number of days used to average a backfill correction
            min_visits_to_fill: minimum number of total visits needed in order to sum a bin

        Returns: dataframes of adjusted covid counts, adjusted visit counts, inclusion array
        """
        revden = den[::-1].values
        revnum = num[::-1].values.reshape(-1, 1)
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

        # reset date index and format
        new_num = pd.Series(new_num.flatten(), name=num.name, index=num.index)
        new_den = pd.Series(new_den, index=den.index)

        return new_num, new_den

    @staticmethod
    def fit(y_data, sensor_dates, geo_id):
        """Fitting routine.

        Args:
            y_data: dataframe for one geo_id, indexed by date
            sensor_dates: list of sorted datetime for which to produce sensor values
            geo_id: unique identifier for the location column

        Returns:
            dictionary of results

        """
        # values to keep
        fitting_idxs = np.where(y_data.index >= sensor_dates[0])[0]

        # backfill
        total_counts, total_visits = EMRHospSensor.backfill(y_data["num"], y_data["den"])

        # calculate smoothed counts and jeffreys rate
        # the left_gauss_linear smoother is not guaranteed to return values greater than 0
        smoothed_total_counts = np.clip(left_gauss_linear(total_counts.values), 0, None)
        smoothed_total_visits = np.clip(left_gauss_linear(total_visits.values), 0, None)
        smoothed_total_rates = (
            (smoothed_total_counts + 0.5) / (smoothed_total_visits + 1)
        )

        # checks - due to the smoother, the first value will be NA
        assert (
            np.sum(np.isnan(smoothed_total_rates[1:]) == True) == 0
        ), "NAs in rate calculation"
        assert (
            np.sum(smoothed_total_rates[1:] <= 0) == 0
        ), f"0 or negative value, {geo_id}"

        # cut off at sensor indexes
        rates = smoothed_total_rates[fitting_idxs]
        den = smoothed_total_visits[fitting_idxs]
        include = den >= Config.MIN_DEN

        # calculate standard error
        se = np.full_like(rates, np.nan)
        se[include] = np.sqrt(
            np.divide((rates[include] * (1 - rates[include])), den[include]))

        logging.debug(f"{geo_id}: {rates[-1]:.3f},[{se[-1]:.3f}]")
        return {"geo_id": geo_id, "rate": 100 * rates, "se": 100 * se, "incl": include}
