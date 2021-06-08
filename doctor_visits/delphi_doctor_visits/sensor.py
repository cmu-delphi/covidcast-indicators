"""
Sensor class to fit a signal using CLI counts from doctor visits.

Author: Maria Jahja
Created: 2020-04-17

"""

# standard packages
import logging

# third party
import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler

# first party
from .config import Config
from .smooth import left_gauss_linear


class DoctorVisitsSensor:
    """Sensor class to fit a signal using CLI counts from doctor visits."""

    @staticmethod
    def transform(
            sig, h=Config.SMOOTHER_BANDWIDTH, smoother=left_gauss_linear, base=None
    ):
        """Transform signal by applying a smoother, and/or adjusting by a base.

        Args:
            signal: 1D signal to transform
            h: smoothing bandwidth
            base: signal to adjust arr with

        Returns: smoothed and/or adjusted 1D signal
        """
        scaler = MinMaxScaler(feature_range=(0, 1))
        sc_sig = scaler.fit_transform(sig)
        sm_sig = smoother(sc_sig, h)

        if base is not None:
            base_scaler = MinMaxScaler(feature_range=(0, 1))
            base = base_scaler.fit_transform(base)
            sc_base = smoother(base, h)
            sm_sig = np.clip(sm_sig - sc_base, 0, None)
        else:
            sm_sig = np.clip(sm_sig, 0, None)

        return scaler.inverse_transform(sm_sig)

    @staticmethod
    def fill_dates(y_data, dates):
        """Ensure all dates are listed in the data, otherwise, add days with 0 counts.

        Args:
            y_data: dataframe with datetime index
            dates: list of datetime to include

        Returns: dataframe containing all dates given
        """
        first_date = dates[0]
        last_date = dates[-1]
        cols = y_data.columns

        if first_date not in y_data.index:
            y_data = y_data.append(
                pd.DataFrame(dict.fromkeys(cols, 0.0), columns=cols, index=[first_date])
            )
        if last_date not in y_data.index:
            y_data = y_data.append(
                pd.DataFrame(dict.fromkeys(cols, 0.0), columns=cols, index=[last_date])
            )

        y_data.sort_index(inplace=True)
        y_data = y_data.asfreq("D", fill_value=0)
        return y_data

    @staticmethod
    def backfill(
            num,
            den,
            k=Config.MAX_BACKFILL_WINDOW,
            min_visits_to_fill=Config.MIN_CUM_VISITS,
            min_visits_to_include=Config.MIN_RECENT_VISITS,
            min_recent_obs_to_include=Config.MIN_RECENT_OBS,
    ):
        """
        Adjust for retroactively added observations.

        Use a variable length smoother, which starts from the RHS and moves
         leftwards (backwards through time). We cumulatively sum the total
         visits (denominator), until we have observed some minimum number of
         counts, then calculate the sum over that bin. We restrict the
         bin size so to avoid inluding long-past values.

        Args:
            num: dataframe of covid counts
            den: dataframe of total visits
            k: maximum number of days used to average a backfill correction
            min_visits_to_fill: minimum number of total visits needed in order to sum a bin
            min_visits_to_include: minimum number of total visits needed to include a date
            min_recent_obs_to_include: day window where we need to observe at least 1 count
                                                                 (inclusive)

        Returns: dataframes of adjusted covid counts, adjusted visit counts, inclusion array
        """
        revden = den[::-1].values
        revnum = num[::-1].values
        new_num = np.full_like(num, np.nan, dtype=float)
        new_den = np.full_like(den, np.nan, dtype=float)
        include = np.full_like(den, True, dtype=bool)
        n, p = num.shape

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

            # if we do not observe at least min_visits_to_include in the denominator or
            # if we observe 0 counts for min_recent_obs window, don't show.
            if (new_den[i] < min_visits_to_include) or (
                    revden[i:][:min_recent_obs_to_include].sum() == 0
            ):
                include[i] = False

        new_num = new_num[::-1]
        new_den = new_den[::-1]
        include = include[::-1]

        # reset date index and format
        new_num = pd.DataFrame(new_num, columns=num.columns)
        new_num.set_index(num.index, inplace=True)
        new_den = pd.Series(new_den)
        new_den.index = den.index

        return new_num, new_den, include

    @staticmethod
    def fit(y_data,
            fit_dates,
            burn_in_dates,
            final_sensor_idxs,
            geo_id,
            recent_min_visits,
            min_recent_obs,
            jeffreys):
        """Fitting routine.

        Args:
            y_data: dataframe for one geo_id, with all 7 cols
            fit_dates: list of sorted datetime for which to use as training
            burn_in_dates: list of sorted datetime for which to produce sensor values
            final_sensor_idxs: list of positions in `fit_dates` that correspond to sensors
            geo_id: unique identifier for the location column
            recent_min_visits: location is sparse if it has fewer than min_recent_visits over
                                                <RECENT_LENGTH> days
            min_recent_obs: location is sparse also if it has 0 observations in the
                                            last min_recent_obs days
            jeffreys: boolean whether to use Jeffreys estimate for binomial proportion, this
                is currently only applied if we are writing SEs out. The estimate is
                p_hat = (x + 0.5)/(n + 1).

        Returns: dictionary of results
        """
        y_data.set_index("ServiceDate", inplace=True)
        y_data = DoctorVisitsSensor.fill_dates(y_data, fit_dates)
        sensor_idxs = np.where(y_data.index >= burn_in_dates[0])[0]
        n_dates = y_data.shape[0]

        # combine Flu_like and Mixed columns
        y_data["Flu_like_Mixed"] = y_data["Flu_like"] + y_data["Mixed"]
        NEW_CLI_COLS = list(set(Config.CLI_COLS) - {"Flu_like", "Mixed"}) + [
            "Flu_like_Mixed"]

        # small backfill correction
        total_visits = y_data["Denominator"]
        total_counts = y_data[NEW_CLI_COLS + Config.FLU1_COL]
        total_counts, total_visits, include = DoctorVisitsSensor.backfill(
            total_counts,
            total_visits,
            min_visits_to_include=recent_min_visits,
            min_recent_obs_to_include=min_recent_obs
        )

        # jeffreys inflation
        if jeffreys:
            total_counts[NEW_CLI_COLS] = total_counts[NEW_CLI_COLS] + 0.5
            total_rates = total_counts.div(total_visits + 1, axis=0)
        else:
            total_rates = total_counts.div(total_visits, axis=0)

        total_rates.fillna(0, inplace=True)
        flu1 = total_rates[Config.FLU1_COL]
        new_rates = []
        for code in NEW_CLI_COLS:
            code_vals = total_rates[code]

            # if all rates are zero, don't bother
            if code_vals.sum() == 0:
                if jeffreys:
                    logging.error("p is 0 even though we used Jefferys estimate")
                new_rates.append(np.zeros((n_dates,)))
                continue

            # include adjustment for flu like codes
            base = flu1 if code in ["Flu_like_Mixed"] else None
            fitted_codes = DoctorVisitsSensor.transform(
                code_vals.values.reshape(-1, 1), base=base
            )
            new_rates.append(fitted_codes.flatten())

        new_rates = np.array(new_rates).sum(axis=0)

        # cut off at sensor indexes
        new_rates = new_rates[sensor_idxs]
        include = include[sensor_idxs]
        den = total_visits[sensor_idxs].values

        # calculate standard error
        se = np.full_like(new_rates, np.nan)
        se[include] = np.sqrt(
            np.divide((new_rates[include] * (1 - new_rates[include])), den[include]))

        logging.debug(f"{geo_id}: {new_rates[-1]:.3f},[{se[-1]:.3f}]")

        included_indices = [x for x in final_sensor_idxs if include[x]]

        df = pd.DataFrame(data = {"date": burn_in_dates[included_indices],
                                  "geo_id": geo_id,
                                  "val": new_rates[included_indices],
                                  "se": se[included_indices]})
        return df
