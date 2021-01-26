"""Dynamic file checks."""
import math
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Dict, Set
import pandas as pd
from .errors import ValidationFailure, APIDataFetchError
from .datafetcher import get_geo_signal_combos, threaded_api_calls
from .utils import relative_difference_by_min, TimeWindow

class DynamicValidator:
    """Class for validation of static properties of individual datasets."""

    @dataclass
    class Parameters:
        """Configuration parameters."""
        # data source name, one of
        # https://cmu-delphi.github.io/delphi-epidata/api/covidcast_signals.html
        data_source: str
        # span of time over which to perform checks
        time_window: TimeWindow
        # date that this df_to_test was generated; typically 1 day after the last date in df_to_test
        generation_date: date
        # number of days back to perform sanity checks, starting from the last date appearing in
        # df_to_test
        max_check_lookbehind: timedelta
        # names of signals that are smoothed (7-day avg, etc)
        smoothed_signals: Set[str]
        # how many days behind do we expect each signal to be
        expected_lag: Dict[str, int]

    def __init__(self, params):
        """
        Initialize object and set parameters.

        Arguments:
            - params: dictionary of user settings; if empty, defaults will be used
        """
        global_params = params["global"]
        dynamic_params = params.get("dynamic", dict())

        self.test_mode = dynamic_params.get("test_mode", False)

        self.params = self.Parameters(
            data_source = global_params["data_source"],
            time_window = TimeWindow.from_params(global_params["end_date"],
                                                 global_params["span_length"]),
            generation_date = date.today(),
            max_check_lookbehind = timedelta(days=dynamic_params.get("ref_window_size", 7)),
            smoothed_signals = set(dynamic_params.get("smoothed_signals", [])),
            expected_lag = dynamic_params.get("expected_lag", dict())
        )


    def validate(self, all_frames, report):
        """
        Performs all checks over the combined data set from all files.

        Parameters
        ----------
        all_frames: pd.DataFrame
            combined data from all input files
        report: ValidationReport
            report to which the results of these checks will be added
        """
        # recent_lookbehind: start from the check date and working backward in time,
        # how many days at a time do we want to check for anomalies?
        # Choosing 1 day checks just the daily data.
        recent_lookbehind = timedelta(days=1)

        # semirecent_lookbehind: starting from the check date and working backward
        # in time, how many days do we use to form the reference statistics.
        semirecent_lookbehind = timedelta(days=7)

        # Get 14 days prior to the earliest list date
        outlier_lookbehind = timedelta(days=14)

        # Get all expected combinations of geo_type and signal.
        geo_signal_combos = get_geo_signal_combos(self.params.data_source)

        all_api_df = threaded_api_calls(self.params.data_source,
                                        self.params.time_window.start_date - outlier_lookbehind,
                                        self.params.time_window.end_date,
                                        geo_signal_combos)

        # Keeps script from checking all files in a test run.
        kroc = 0

        # Comparison checks
        # Run checks for recent dates in each geo-sig combo vs semirecent (previous
        # week) API data.
        for geo_type, signal_type in geo_signal_combos:
            geo_sig_df = all_frames.query(
                "geo_type == @geo_type & signal == @signal_type")
            # Drop unused columns.
            geo_sig_df.drop(columns=["geo_type", "signal"])

            report.increment_total_checks()

            if geo_sig_df.empty:
                report.add_raised_error(ValidationFailure("check_missing_geo_sig_combo",
                                                          f"{geo_type} {signal_type}",
                                                          "file with geo_type-signal combo does "
                                                          "not exist"))
                continue

            max_date = geo_sig_df["time_value"].max()
            self.check_min_allowed_max_date(max_date, geo_type, signal_type, report)
            self.check_max_allowed_max_date(max_date, geo_type, signal_type, report)

            # Get relevant reference data from API dictionary.
            api_df_or_error = all_api_df[(geo_type, signal_type)]

            report.increment_total_checks()
            if isinstance(api_df_or_error, APIDataFetchError):
                report.raised_errors.append(api_df_or_error)
                continue

            # Outlier dataframe
            if (signal_type in ["confirmed_7dav_cumulative_num", "confirmed_7dav_incidence_num",
                                "confirmed_cumulative_num", "confirmed_incidence_num",
                                "deaths_7dav_cumulative_num",
                                "deaths_cumulative_num"]):
                earliest_available_date = geo_sig_df["time_value"].min()
                source_df = geo_sig_df.query(
                    'time_value <= @self.params.time_window.end_date & '
                    'time_value >= @self.params.time_window.start_date'
                )

                # These variables are interpolated into the call to `api_df_or_error.query()`
                # below but pylint doesn't recognize that.
                # pylint: disable=unused-variable
                outlier_start_date = earliest_available_date - outlier_lookbehind
                outlier_end_date = earliest_available_date - timedelta(days=1)
                outlier_api_df = api_df_or_error.query(
                    'time_value <= @outlier_end_date & time_value >= @outlier_start_date')
                # pylint: enable=unused-variable

                self.check_positive_negative_spikes(
                    source_df, outlier_api_df, geo_type, signal_type, report)

            # Check data from a group of dates against recent (previous 7 days,
            # by default) data from the API.
            for checking_date in self.params.time_window.date_seq:
                recent_cutoff_date = checking_date - \
                    recent_lookbehind + timedelta(days=1)
                recent_df = geo_sig_df.query(
                    'time_value <= @checking_date & time_value >= @recent_cutoff_date')

                report.increment_total_checks()

                if recent_df.empty:
                    report.add_raised_error(
                        ValidationFailure("check_missing_geo_sig_date_combo",
                                          f"{checking_date} {geo_type} {signal_type}",
                                          "test data for a given checking date-geo type-signal type"
                                          " combination is missing. Source data may be missing"
                                          " for one or more dates"))
                    continue

                # Reference dataframe runs backwards from the recent_cutoff_date
                #
                # These variables are interpolated into the call to `api_df_or_error.query()`
                # below but pylint doesn't recognize that.
                # pylint: disable=unused-variable
                reference_start_date = recent_cutoff_date - \
                    min(semirecent_lookbehind, self.params.max_check_lookbehind) - \
                    timedelta(days=1)
                reference_end_date = recent_cutoff_date - timedelta(days=1)
                # pylint: enable=unused-variable

                # Subset API data to relevant range of dates.
                reference_api_df = api_df_or_error.query(
                    "time_value >= @reference_start_date & time_value <= @reference_end_date")

                report.increment_total_checks()

                if reference_api_df.empty:
                    report.add_raised_error(
                        ValidationFailure("empty_reference_data",
                                          f"{checking_date} {geo_type} {signal_type}",
                                          "reference data is empty; comparative checks could not "
                                          "be performed"))
                    continue

                self.check_max_date_vs_reference(
                    recent_df, reference_api_df, checking_date, geo_type, signal_type, report)

                self.check_rapid_change_num_rows(
                    recent_df, reference_api_df, checking_date, geo_type, signal_type, report)

                self.check_avg_val_vs_reference(
                    recent_df, reference_api_df, checking_date, geo_type, signal_type, report)

            # Keeps script from checking all files in a test run.
            kroc += 1
            if self.test_mode and kroc == 2:
                break

    def check_min_allowed_max_date(self, max_date, geo_type, signal_type, report):
        """
        Check if time since data was generated is reasonable or too long ago.

        Arguments:
            - max_date: date of most recent data to be validated; datetime format.
            - geo_type: str; geo type name (county, msa, hrr, state) as in the CSV name
            - signal_type: str; signal name as in the CSV name
            - report: ValidationReport; report where results are added

        Returns:
            - None
        """
        thres = timedelta(
            days=self.params.expected_lag[signal_type] if signal_type in self.params.expected_lag
            else 1)

        if max_date < self.params.generation_date - thres:
            report.add_raised_error(
                ValidationFailure("check_min_max_date",
                                  f"{geo_type} {signal_type}",
                                  "date of most recent generated file seems too long ago"))

        report.increment_total_checks()

    def check_max_allowed_max_date(self, max_date, geo_type, signal_type, report):
        """
        Check if time since data was generated is reasonable or too recent.

        Arguments:
            - max_date: date of most recent data to be validated; datetime format.
            - geo_type: str; geo type name (county, msa, hrr, state) as in the CSV name
            - signal_type: str; signal name as in the CSV name
            - report: ValidationReport; report where results are added

        Returns:
            - None
        """
        if max_date > self.params.generation_date:
            report.add_raised_error(
                ValidationFailure("check_max_max_date",
                                  f"{geo_type} {signal_type}",
                                  "date of most recent generated file seems too recent"))

        report.increment_total_checks()

    def check_max_date_vs_reference(self, df_to_test, df_to_reference, checking_date,
                                    geo_type, signal_type, report):
        """
        Check if reference data is more recent than test data.

        Arguments:
            - df_to_test: pandas dataframe of a single CSV of source data
            (one day-signal-geo_type combo)
            - df_to_reference: pandas dataframe of reference data, either from the
            COVIDcast API or semirecent data
            - geo_type: str; geo type name (county, msa, hrr, state) as in the CSV name
            - signal_type: str; signal name as in the CSV name
            - report: ValidationReport; report where results are added

        Returns:
            - None
        """
        if df_to_test["time_value"].max() < df_to_reference["time_value"].max():
            report.add_raised_error(
                ValidationFailure("check_max_date_vs_reference",
                                  f"{checking_date.date()} {geo_type} {signal_type}",
                                  "reference df has days beyond the max date in the =df_to_test="))

        report.increment_total_checks()

    def check_rapid_change_num_rows(self, df_to_test, df_to_reference, checking_date,
                                    geo_type, signal_type, report):
        """
        Compare number of obervations per day in test dataframe vs reference dataframe.

        Arguments:
            - df_to_test: pandas dataframe of CSV source data
            - df_to_reference: pandas dataframe of reference data, either from the
            COVIDcast API or semirecent data
            - checking_date: datetime date
            - geo_type: str; geo type name (county, msa, hrr, state) as in the CSV name
            - signal_type: str; signal name as in the CSV name
            - report: ValidationReport; report where results are added

        Returns:
            - None
        """
        test_rows_per_reporting_day = df_to_test[df_to_test['time_value']
                                                 == checking_date].shape[0]
        reference_rows_per_reporting_day = df_to_reference.shape[0] / len(
            set(df_to_reference["time_value"]))

        try:
            compare_rows = relative_difference_by_min(
                test_rows_per_reporting_day,
                reference_rows_per_reporting_day)
        except ZeroDivisionError as e:
            print(checking_date, geo_type, signal_type)
            raise e

        if abs(compare_rows) > 0.35:
            report.add_raised_error(
                ValidationFailure("check_rapid_change_num_rows",
                                  f"{checking_date} {geo_type} {signal_type}",
                                  "Number of rows per day seems to have changed rapidly (reference "
                                  "vs test data)"))

        report.increment_total_checks()

    def check_positive_negative_spikes(self, source_df, api_frames, geo, sig, report):
        """
        Adapt Dan's corrections package to Python (only consider spikes) :
        https://github.com/cmu-delphi/covidcast-forecast/tree/dev/corrections/data_corrections

        Statistics for a right shifted rolling window and a centered rolling window are used
        to determine outliers for both positive and negative spikes.

        As it is now, ststat will always be NaN for source frames.

        Arguments:
            - source_df: pandas dataframe of CSV source data
            - api_frames: pandas dataframe of reference data, either from the
            COVIDcast API or semirecent data
            - geo: str; geo type name (county, msa, hrr, state) as in the CSV name
            - sig: str; signal name as in the CSV name
            - report: ValidationReport; report where results are added

        """
        report.increment_total_checks()
        # Combine all possible frames so that the rolling window calculations make sense.
        source_frame_start = source_df["time_value"].min()
        source_frame_end = source_df["time_value"].max()
        all_frames = pd.concat([api_frames, source_df]). \
            drop_duplicates(subset=["geo_id", "time_value"], keep='last'). \
            sort_values(by=['time_value']).reset_index(drop=True)

        # Tuned Variables from Dan's Code for flagging outliers. Size_cut is a
        # check on the minimum value reported, sig_cut is a check
        # on the ftstat or ststat reported (t-statistics) and sig_consec
        # is a lower check for determining outliers that are next to each other.
        size_cut = 5
        sig_cut = 3
        sig_consec = 2.25

        # Functions mapped to rows to determine outliers based on fstat and ststat values

        def outlier_flag(frame):
            if (abs(frame["val"]) > size_cut) and not (pd.isna(frame["ststat"])) \
                    and (frame["ststat"] > sig_cut):
                return True
            if (abs(frame["val"]) > size_cut) and (pd.isna(frame["ststat"])) and \
                    not (pd.isna(frame["ftstat"])) and (frame["ftstat"] > sig_cut):
                return True
            if (frame["val"] < -size_cut) and not (pd.isna(frame["ststat"])) and \
                    not pd.isna(frame["ftstat"]):
                return True
            return False

        def outlier_nearby(frame):
            if (not pd.isna(frame['ststat'])) and (frame['ststat'] > sig_consec):
                return True
            if pd.isna(frame['ststat']) and (frame['ftstat'] > sig_consec):
                return True
            return False

        # Calculate ftstat and ststat values for the rolling windows, group fames by geo region
        region_group = all_frames.groupby("geo_id")
        window_size = 14
        # Shift the window to match how R calculates rolling windows with even numbers
        shift_val = -1 if window_size % 2 == 0 else 0

        # Calculate the t-statistics for the two rolling windows (windows center and windows right)
        all_full_frames = []
        for _, group in region_group:
            rolling_windows = group["val"].rolling(window_size, min_periods=window_size)
            center_windows = group["val"].rolling(window_size, min_periods=window_size, center=True)
            fmedian = rolling_windows.median()
            smedian = center_windows.median().shift(shift_val)
            fsd = rolling_windows.std() + 0.00001  # if std is 0
            ssd = center_windows.std().shift(shift_val) + 0.00001  # if std is 0
            group['ftstat'] = abs(group["val"] - fmedian.fillna(0)) / fsd
            group['ststat'] = abs(group["val"] - smedian.fillna(0)) / ssd
            all_full_frames.append(group)

        all_frames = pd.concat(all_full_frames)
        # Determine outliers in source frames only, only need the reference
        # data from just before the start of the source data
        # because lead and lag outlier calculations are only one day
        #
        # These variables are interpolated into the call to `api_df_or_error.query()`
        # below but pylint doesn't recognize that.
        # pylint: disable=unused-variable
        api_frames_end = min(api_frames["time_value"].max(), source_frame_start-timedelta(days=1))
        # pylint: enable=unused-variable
        outlier_df = all_frames.query(
            'time_value >= @api_frames_end & time_value <= @source_frame_end')
        outlier_df = outlier_df.sort_values(by=['geo_id', 'time_value']) \
            .reset_index(drop=True).copy()
        outliers = outlier_df[outlier_df.apply(outlier_flag, axis=1)]
        outliers_reset = outliers.copy().reset_index(drop=True)

        # Find the lead outliers and the lag outliers. Check that the selected row
        # is actually a leading and lagging row for given geo_id
        upper_index = list(filter(lambda x: x < outlier_df.shape[0],
                                  list(outliers.index+1)))
        upper_df = outlier_df.iloc[upper_index, :].reset_index(drop=True)
        upper_compare = outliers_reset[:len(upper_index)]
        sel_upper_df = upper_df[upper_compare["geo_id"]
                                == upper_df["geo_id"]].copy()
        lower_index = list(filter(lambda x: x >= 0, list(outliers.index-1)))
        lower_df = outlier_df.iloc[lower_index, :].reset_index(drop=True)
        lower_compare = outliers_reset[-len(lower_index):].reset_index(drop=True)
        sel_lower_df = lower_df[lower_compare["geo_id"]
                                == lower_df["geo_id"]].copy()

        outliers_list = [outliers]
        if sel_upper_df.size > 0:
            outliers_list.append(sel_upper_df[sel_upper_df.apply(outlier_nearby, axis=1)])
        if sel_lower_df.size > 0:
            outliers_list.append(sel_lower_df[sel_lower_df.apply(outlier_nearby, axis=1)])

        all_outliers = pd.concat(outliers_list). \
            sort_values(by=['time_value', 'geo_id']). \
            drop_duplicates().reset_index(drop=True)

        # Identify outliers just in the source data
        source_outliers = all_outliers.query(
            "time_value >= @source_frame_start & time_value <= @source_frame_end")

        if source_outliers.shape[0] > 0:
            report.raised_errors.append(
                ValidationFailure(
                    "check_positive_negative_spikes",
                    f"{source_frame_start} {source_frame_end} {geo} {sig}",
                    "Source dates with flagged ouliers based on the previous 14 days of data "
                    "available"))

    def check_avg_val_vs_reference(self, df_to_test, df_to_reference, checking_date, geo_type,
                                   signal_type, report):
        """
        Compare average values for each variable in test dataframe vs reference dataframe.
        Arguments:
            - df_to_test: pandas dataframe of CSV source data
            - df_to_reference: pandas dataframe of reference data, either from the
            COVIDcast API or semirecent data
            - geo_type: str; geo type name (county, msa, hrr, state) as in the CSV name
            - signal_type: str; signal name as in the CSV name
            - report: ValidationReport; report where results are added
        Returns:
            - None
        """
        # Average each of val, se, and sample_size over all dates for a given geo_id.
        # Ignores NA by default.
        df_to_test = df_to_test.groupby(['geo_id'], as_index=False)[
            ['val', 'se', 'sample_size']].mean()
        df_to_test["type"] = "test"

        df_to_reference = df_to_reference.groupby(['geo_id'], as_index=False)[
            ['val', 'se', 'sample_size']].mean()
        df_to_reference["type"] = "reference"

        df_all = pd.concat([df_to_test, df_to_reference])

        # For each variable (val, se, and sample size) where not missing, calculate the
        # relative mean difference and mean absolute difference between the test data
        # and the reference data across all geographic regions.
        #
        # Steps:
        #   - melt: creates a long version of df, where 'variable' specifies variable
        # name (val, se, sample size) and 'value' specifies the value of said variable;
        # geo_id and type columns are unchanged
        #   - pivot: each row is the test and reference values for a given geo
        # region-variable type combo
        #   - reset_index: index is set to auto-incrementing int; geo_id and variable
        # names are back as normal columns
        #   - dropna: drop all rows with at least one missing value (makes it
        # impossible to compare reference and test)
        #   - assign: create new temporary columns, raw and abs value of difference
        # between test and reference columns
        #   - groupby: group by variable name
        #   - agg: for every variable name group (across geo regions), calculate the
        # mean of each of the raw difference between test and reference columns, the
        # abs value of the difference between test and reference columns, all test
        # values, all reference values
        #   - assign: use the new aggregate vars to calculate the relative mean
        # difference, 2 * mean(differences) / sum(means) of two groups.
        df_all = pd.melt(
            df_all, id_vars=["geo_id", "type"], value_vars=["val", "se", "sample_size"]
        ).pivot(
            index=("geo_id", "variable"), columns="type", values="value"
        ).reset_index(
            ("geo_id", "variable")
        ).dropna(
        ).assign(
            type_diff=lambda x: x["test"] - x["reference"],
            abs_type_diff=lambda x: abs(x["type_diff"])
        ).groupby(
            "variable", as_index=False
        ).agg(
            mean_type_diff=("type_diff", "mean"),
            mean_abs_type_diff=("abs_type_diff", "mean"),
            mean_test_var=("test", "mean"),
            mean_ref_var=("reference", "mean")
        ).assign(
            mean_stddiff=lambda x: 2 *
            x["mean_type_diff"] / (x["mean_test_var"] + x["mean_ref_var"]),
            mean_stdabsdiff=lambda x: 2 *
            x["mean_abs_type_diff"] / (x["mean_test_var"] + x["mean_ref_var"])
        )[["variable", "mean_stddiff", "mean_stdabsdiff"]]

        # Set thresholds for raw and smoothed variables.
        classes = ['mean_stddiff', 'val_mean_stddiff', 'mean_stdabsdiff']
        raw_thresholds = pd.DataFrame([[1.50, 1.30, 1.80]], columns=classes)
        smoothed_thresholds = raw_thresholds.apply(lambda x: x/(math.sqrt(7) * 1.5))

        switcher = {
            'raw': raw_thresholds,
            'smoothed': smoothed_thresholds,
        }

        # Get the selected thresholds from switcher dictionary
        smooth_option = "smoothed" if signal_type in self.params.smoothed_signals else "raw"
        thres = switcher.get(smooth_option, lambda: "Invalid smoothing option")

        # Check if the calculated mean differences are high compared to the thresholds.
        mean_stddiff_high = (
            abs(df_all["mean_stddiff"]) > float(thres["mean_stddiff"])).any() or (
                (df_all["variable"] == "val").any() and
                (abs(df_all[df_all["variable"] == "val"]["mean_stddiff"])
                 > float(thres["val_mean_stddiff"])).any()
        )
        mean_stdabsdiff_high = (df_all["mean_stdabsdiff"] > float(thres["mean_stdabsdiff"])).any()

        if mean_stddiff_high or mean_stdabsdiff_high:
            report.add_raised_error(
                ValidationFailure(
                    "check_test_vs_reference_avg_changed",
                    f"{checking_date} {geo_type} {signal_type}",
                    'Average differences in variables by geo_id between recent & reference data '
                    + 'seem large --- either large increase '
                    + 'tending toward one direction or large mean absolute difference, relative '
                    + 'to average values of corresponding variables. For the former check, '
                    + 'tolerances for `val` are more restrictive than those for other columns.'))

        report.increment_total_checks()
