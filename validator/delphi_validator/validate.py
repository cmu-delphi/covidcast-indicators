# -*- coding: utf-8 -*-
"""
Tools to validate CSV source data, including various check methods.
"""
import sys
import re
import math
from os.path import join
from datetime import date, datetime, timedelta
import pandas as pd
from .errors import ValidationError, APIDataFetchError
from .datafetcher import FILENAME_REGEX, get_geo_signal_combos, threaded_api_calls, load_all_files
from .utils import GEO_REGEX_DICT, relative_difference_by_min, aggregate_frames

class Validator():
    """ Class containing validation() function and supporting functions. Stores a list
    of all raised errors, and user settings. """

    def __init__(self, params):
        """
        Initialize object and set parameters.

        Arguments:
            - params: dictionary of user settings; if empty, defaults will be used

        Attributes:
            - data_source: str; data source name, one of
            https://cmu-delphi.github.io/delphi-epidata/api/covidcast_signals.html
            - start_date: beginning date of data to check, in datetime date format
            - span_length: number of days before the end date to include in checking
            - end_date: end date of data to check, in datetime date format
            - generation_date: date that this df_to_test was generated; typically 1 day
            after the last date in df_to_test
            - max_check_lookbehind: number of days back to perform sanity checks, starting
            from the last date appearing in df_to_test
            - minimum_sample_size: int
            - missing_se_allowed: boolean indicating if missing standard errors should
            raise an exception or not
            - missing_sample_size_allowed: boolean indicating if missing sample size should
            raise an exception or not
            - sanity_check_rows_per_day: boolean; check flag
            - sanity_check_value_diffs: boolean; check flag
            - smoothed_signals: set of strings; names of signals that are smoothed (7-day
            avg, etc)
            - expected_lag: dict of signal names: int pairs; how many days behind do we
            expect each signal to be
            - suppressed_errors: set of check_data_ids used to identify error messages to ignore
            - raised_errors: list to append data upload-blocking errors to as they are raised
            - total_checks: incremental counter to track total number of checks run
            - raised_warnings: list to append non-data upload-blocking errors to as they are raised
        """
        # TODO(https://github.com/cmu-delphi/covidcast-indicators/issues/579)
        # Refactor this class to avoid the too-many-instance-attributes error.
        #
        # pylint: disable=too-many-instance-attributes

        # Get user settings from params or if not provided, set default.
        self.data_source = params['data_source']
        self.validator_static_file_dir = params.get(
            'validator_static_file_dir', '../validator/static')

        # Date/time settings
        self.span_length = timedelta(days=params['span_length'])
        self.end_date = date.today() if params['end_date'] == "latest" else datetime.strptime(
            params['end_date'], '%Y-%m-%d').date()
        self.start_date = self.end_date - self.span_length
        self.generation_date = date.today()

        # General options: flags, thresholds
        self.max_check_lookbehind = timedelta(
            days=params.get("ref_window_size", 7))
        self.minimum_sample_size = params.get('minimum_sample_size', 100)
        self.missing_se_allowed = params.get('missing_se_allowed', False)
        self.missing_sample_size_allowed = params.get(
            'missing_sample_size_allowed', False)

        self.sanity_check_rows_per_day = params.get(
            'sanity_check_rows_per_day', True)
        self.sanity_check_value_diffs = params.get(
            'sanity_check_value_diffs', True)
        self.test_mode = params.get("test_mode", False)

        # Signal-specific settings
        self.smoothed_signals = set(params.get("smoothed_signals", []))
        self.expected_lag = params["expected_lag"]

        self.suppressed_errors = {(item,) if not isinstance(item, tuple) and not isinstance(
            item, list) else tuple(item) for item in params.get('suppressed_errors', [])}

        # Output
        self.raised_errors = []
        self.total_checks = 0

        self.raised_warnings = []
        # pylint:  enable=too-many-instance-attributes

    def increment_total_checks(self):
        """ Add 1 to total_checks counter """
        self.total_checks += 1

    def check_missing_date_files(self, daily_filenames):
        """
        Check for missing dates between the specified start and end dates.

        Arguments:
            - daily_filenames: List[Tuple(str, re.match, pd.DataFrame)]
                triples of filenames, filename matches with the geo regex, and the data from the
                file

        Returns:
            - None
        """
        number_of_dates = self.end_date - self.start_date + timedelta(days=1)

        # Create set of all expected dates.
        date_seq = {self.start_date + timedelta(days=x)
                    for x in range(number_of_dates.days)}
        # Create set of all dates seen in CSV names.
        unique_dates = {datetime.strptime(
            daily_filename[0][0:8], '%Y%m%d').date() for daily_filename in daily_filenames}

        # Diff expected and observed dates.
        check_dateholes = list(date_seq.difference(unique_dates))
        check_dateholes.sort()

        if check_dateholes:
            self.raised_errors.append(ValidationError(
                "check_missing_date_files",
                check_dateholes,
                "Missing dates are observed; if these dates are" +
                " already in the API they would not be updated"))

        self.increment_total_checks()

    def check_df_format(self, df_to_test, nameformat):
        """
        Check basic format of source data CSV df.

        Arguments:
            - df_to_test: pandas dataframe of a single CSV of source data
            (one day-signal-geo_type combo)
            - nameformat: str CSV name; for example, "20200624_county_smoothed_nohh_cmnty_cli.csv"

        Returns:
            - None
        """
        pattern_found = FILENAME_REGEX.match(nameformat)
        if not nameformat or not pattern_found:
            self.raised_errors.append(ValidationError(
                ("check_filename_format", nameformat),
                nameformat, 'nameformat not recognized'))

        self.increment_total_checks()

        if not isinstance(df_to_test, pd.DataFrame):
            self.raised_errors.append(ValidationError(
                ("check_file_data_format", nameformat),
                type(df_to_test), 'df_to_test must be a pandas dataframe.'))

        self.increment_total_checks()

    def check_bad_geo_id_value(self, df_to_test, filename, geo_type):
        """
        Check for bad geo_id values, by comparing to a list of known values (drawn from
        historical data)

        Arguments:
            - df_to_test: pandas dataframe of CSV source data containing the geo_id column to check
            - geo_type: string from CSV name specifying geo type (state, county, msa, etc.) of data
        """
        file_path = join(self.validator_static_file_dir, geo_type + '_geo.csv')
        valid_geo_df = pd.read_csv(file_path, dtype={'geo_id': str})
        valid_geos = valid_geo_df['geo_id'].values
        unexpected_geos = [geo for geo in df_to_test['geo_id']
                           if geo.lower() not in valid_geos]
        if len(unexpected_geos) > 0:
            self.raised_errors.append(ValidationError(
                ("check_bad_geo_id_value", filename),
                unexpected_geos, "Unrecognized geo_ids (not in historical data)"))
        self.increment_total_checks()
        upper_case_geos = [
            geo for geo in df_to_test['geo_id'] if geo.lower() != geo]
        if len(upper_case_geos) > 0:
            self.raised_warnings.append(ValidationError(
                ("check_geo_id_lowercase", filename),
                upper_case_geos, "geo_id contains uppercase characters. Lowercase is preferred."))
        self.increment_total_checks()

    def check_bad_geo_id_format(self, df_to_test, nameformat, geo_type):
        """
        Check validity of geo_type and format of geo_ids, according to regex pattern.

        Arguments:
            - df_to_test: pandas dataframe of CSV source data
            - geo_type: string from CSV name specifying geo type (state, county, msa, hrr) of data

        Returns:
            - None
        """
        def find_all_unexpected_geo_ids(df_to_test, geo_regex, geo_type):
            """
            Check if any geo_ids in df_to_test aren't formatted correctly, according
            to the geo type dictionary negated_regex_dict.
            """
            numeric_geo_types = {"msa", "county", "hrr", "dma"}
            fill_len = {"msa": 5, "county": 5, "dma": 3}

            if geo_type in numeric_geo_types:
                # Check if geo_ids were stored as floats (contain decimal point) and
                # contents before decimal match the specified regex pattern.
                leftover = [geo[1] for geo in df_to_test["geo_id"].str.split(
                    ".") if len(geo) > 1 and re.match(geo_regex, geo[0])]

                # If any floats found, remove decimal and anything after.
                if len(leftover) > 0:
                    df_to_test["geo_id"] = [geo[0]
                                            for geo in df_to_test["geo_id"].str.split(".")]

                    self.raised_warnings.append(ValidationError(
                        ("check_geo_id_type", nameformat),
                        None, "geo_ids saved as floats; strings preferred"))

            if geo_type in fill_len.keys():
                # Left-pad with zeroes up to expected length. Fixes missing leading zeroes
                # caused by FIPS codes saved as numeric.
                df_to_test["geo_id"] = pd.Series([geo.zfill(fill_len[geo_type])
                                                  for geo in df_to_test["geo_id"]], dtype=str)

            expected_geos = [geo[0] for geo in df_to_test['geo_id'].str.findall(
                geo_regex) if len(geo) > 0]

            unexpected_geos = {geo for geo in set(
                df_to_test['geo_id']) if geo not in expected_geos}

            if len(unexpected_geos) > 0:
                self.raised_errors.append(ValidationError(
                    ("check_geo_id_format", nameformat),
                    unexpected_geos, "Non-conforming geo_ids found"))

        if geo_type not in GEO_REGEX_DICT:
            self.raised_errors.append(ValidationError(
                ("check_geo_type", nameformat),
                geo_type, "Unrecognized geo type"))
        else:
            find_all_unexpected_geo_ids(
                df_to_test, GEO_REGEX_DICT[geo_type], geo_type)

        self.increment_total_checks()

    def check_bad_val(self, df_to_test, nameformat, signal_type):
        """
        Check value field for validity.

        Arguments:
            - df_to_test: pandas dataframe of a single CSV of source data
            - signal_type: string from CSV name specifying signal type (smoothed_cli, etc) of data

        Returns:
            - None
        """
        # Determine if signal is a proportion (# of x out of 100k people) or percent
        percent_option = bool('pct' in signal_type)
        proportion_option = bool('prop' in signal_type)

        if percent_option:
            if not df_to_test[(df_to_test['val'] > 100)].empty:
                self.raised_errors.append(ValidationError(
                    ("check_val_pct_gt_100", nameformat),
                    df_to_test[(df_to_test['val'] > 100)],
                    "val column can't have any cell greater than 100 for percents"))

            self.increment_total_checks()

        if proportion_option:
            if not df_to_test[(df_to_test['val'] > 100000)].empty:
                self.raised_errors.append(ValidationError(
                    ("check_val_prop_gt_100k", nameformat),
                    df_to_test[(df_to_test['val'] > 100000)],
                    "val column can't have any cell greater than 100000 for proportions"))

            self.increment_total_checks()

        if df_to_test['val'].isnull().values.any():
            self.raised_errors.append(ValidationError(
                ("check_val_missing", nameformat),
                None, "val column can't have any cell that is NA"))

        self.increment_total_checks()

        if not df_to_test[(df_to_test['val'] < 0)].empty:
            self.raised_errors.append(ValidationError(
                ("check_val_lt_0", nameformat),
                df_to_test[(df_to_test['val'] < 0)],
                "val column can't have any cell smaller than 0"))

        self.increment_total_checks()

    def check_bad_se(self, df_to_test, nameformat):
        """
        Check standard errors for validity.

        Arguments:
            - df_to_test: pandas dataframe of a single CSV of source data
            (one day-signal-geo_type combo)
            - nameformat: str CSV name; for example, "20200624_county_smoothed_nohh_cmnty_cli.csv"

        Returns:
            - None
        """
        # Add a new se_upper_limit column.
        df_to_test.eval(
            'se_upper_limit = (val * sample_size + 50)/(sample_size + 1)', inplace=True)

        df_to_test['se'] = df_to_test['se'].round(3)
        df_to_test['se_upper_limit'] = df_to_test['se_upper_limit'].round(3)

        if not self.missing_se_allowed:
            # Find rows not in the allowed range for se.
            result = df_to_test.query(
                '~((se > 0) & (se < 50) & (se <= se_upper_limit))')

            if not result.empty:
                self.raised_errors.append(ValidationError(
                    ("check_se_not_missing_and_in_range", nameformat),
                    result, "se must be in (0, min(50,val*(1+eps))] and not missing"))

            self.increment_total_checks()

            if df_to_test["se"].isnull().mean() > 0.5:
                self.raised_errors.append(ValidationError(
                    ("check_se_many_missing", nameformat),
                    None, 'Recent se values are >50% NA'))

            self.increment_total_checks()

        elif self.missing_se_allowed:
            result = df_to_test.query(
                '~(se.isnull() | ((se > 0) & (se < 50) & (se <= se_upper_limit)))')

            if not result.empty:
                self.raised_errors.append(ValidationError(
                    ("check_se_missing_or_in_range", nameformat),
                    result, "se must be NA or in (0, min(50,val*(1+eps))]"))

            self.increment_total_checks()

        result_jeffreys = df_to_test.query('(val == 0) & (se == 0)')
        result_alt = df_to_test.query('se == 0')

        if not result_jeffreys.empty:
            self.raised_errors.append(ValidationError(
                ("check_se_0_when_val_0", nameformat),
                None,
                "when signal value is 0, se must be non-zero. please "
                + "use Jeffreys correction to generate an appropriate se"
                + " (see wikipedia.org/wiki/Binomial_proportion_confidence"
                + "_interval#Jeffreys_interval for details)"))
        elif not result_alt.empty:
            self.raised_errors.append(ValidationError(
                ("check_se_0", nameformat),
                result_alt, "se must be non-zero"))

        self.increment_total_checks()

        # Remove se_upper_limit column.
        df_to_test.drop(columns=["se_upper_limit"])

    def check_bad_sample_size(self, df_to_test, nameformat):
        """
        Check sample sizes for validity.

        Arguments:
            - df_to_test: pandas dataframe of a single CSV of source data
            (one day-signal-geo_type combo)
            - nameformat: str CSV name; for example, "20200624_county_smoothed_nohh_cmnty_cli.csv"

        Returns:
            - None
        """
        if not self.missing_sample_size_allowed:
            if df_to_test['sample_size'].isnull().values.any():
                self.raised_errors.append(ValidationError(
                    ("check_n_missing", nameformat),
                    None, "sample_size must not be NA"))

            self.increment_total_checks()

            # Find rows with sample size less than minimum allowed
            result = df_to_test.query(
                '(sample_size < @self.minimum_sample_size)')

            if not result.empty:
                self.raised_errors.append(ValidationError(
                    ("check_n_gt_min", nameformat),
                    result, f"sample size must be >= {self.minimum_sample_size}"))

            self.increment_total_checks()

        elif self.missing_sample_size_allowed:
            result = df_to_test.query(
                '~(sample_size.isnull() | (sample_size >= @self.minimum_sample_size))')

            if not result.empty:
                self.raised_errors.append(ValidationError(
                    ("check_n_missing_or_gt_min", nameformat),
                    result,
                    f"sample size must be NA or >= {self.minimum_sample_size}"))

            self.increment_total_checks()

    def check_min_allowed_max_date(self, max_date, geo_type, signal_type):
        """
        Check if time since data was generated is reasonable or too long ago.

        Arguments:
            - max_date: date of most recent data to be validated; datetime format.
            - geo_type: str; geo type name (county, msa, hrr, state) as in the CSV name
            - signal_type: str; signal name as in the CSV name

        Returns:
            - None
        """
        thres = timedelta(
            days=self.expected_lag[signal_type] if signal_type in self.expected_lag
            else 1)

        if max_date < self.generation_date - thres:
            self.raised_errors.append(ValidationError(
                ("check_min_max_date", geo_type, signal_type),
                max_date,
                "date of most recent generated file seems too long ago"))

        self.increment_total_checks()

    def check_max_allowed_max_date(self, max_date, geo_type, signal_type):
        """
        Check if time since data was generated is reasonable or too recent.

        Arguments:
            - max_date: date of most recent data to be validated; datetime format.
            - geo_type: str; geo type name (county, msa, hrr, state) as in the CSV name
            - signal_type: str; signal name as in the CSV name

        Returns:
            - None
        """
        if max_date > self.generation_date:
            self.raised_errors.append(ValidationError(
                ("check_max_max_date", geo_type, signal_type),
                max_date,
                "date of most recent generated file seems too recent"))

        self.increment_total_checks()

    def check_max_date_vs_reference(self, df_to_test, df_to_reference, checking_date,
                                    geo_type, signal_type):
        """
        Check if reference data is more recent than test data.

        Arguments:
            - df_to_test: pandas dataframe of a single CSV of source data
            (one day-signal-geo_type combo)
            - df_to_reference: pandas dataframe of reference data, either from the
            COVIDcast API or semirecent data
            - geo_type: str; geo type name (county, msa, hrr, state) as in the CSV name
            - signal_type: str; signal name as in the CSV name

        Returns:
            - None
        """
        if df_to_test["time_value"].max() < df_to_reference["time_value"].max():
            self.raised_errors.append(ValidationError(
                ("check_max_date_vs_reference",
                 checking_date.date(), geo_type, signal_type),
                (df_to_test["time_value"].max(),
                 df_to_reference["time_value"].max()),
                'reference df has days beyond the max date in the =df_to_test=; ' +
                'checks are not constructed to handle this case, and this situation ' +
                'may indicate that something locally is out of date, or, if the local ' +
                'working files have already been compared against the reference, ' +
                'that there is a bug somewhere'))

        self.increment_total_checks()

    def check_rapid_change_num_rows(self, df_to_test, df_to_reference, checking_date,
                                    geo_type, signal_type):
        """
        Compare number of obervations per day in test dataframe vs reference dataframe.

        Arguments:
            - df_to_test: pandas dataframe of CSV source data
            - df_to_reference: pandas dataframe of reference data, either from the
            COVIDcast API or semirecent data
            - checking_date: datetime date
            - geo_type: str; geo type name (county, msa, hrr, state) as in the CSV name
            - signal_type: str; signal name as in the CSV name

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
            self.raised_errors.append(ValidationError(
                ("check_rapid_change_num_rows",
                 checking_date, geo_type, signal_type),
                (test_rows_per_reporting_day, reference_rows_per_reporting_day),
                "Number of rows per day (-with-any-rows) seems to have changed " +
                "rapidly (reference vs test data)"))

        self.increment_total_checks()

    def check_positive_negative_spikes(self, source_df, api_frames, geo, sig):
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

        """
        self.increment_total_checks()
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
            self.raised_errors.append(ValidationError(
                ("check_positive_negative_spikes",
                 source_frame_start, source_frame_end, geo, sig),
                (source_outliers),
                'Source dates with flagged ouliers based on the \
                previous 14 days of data available'))

    def check_avg_val_vs_reference(self, df_to_test, df_to_reference, checking_date, geo_type,
                                   signal_type):
        """
        Compare average values for each variable in test dataframe vs reference dataframe.
        Arguments:
            - df_to_test: pandas dataframe of CSV source data
            - df_to_reference: pandas dataframe of reference data, either from the
            COVIDcast API or semirecent data
            - geo_type: str; geo type name (county, msa, hrr, state) as in the CSV name
            - signal_type: str; signal name as in the CSV name
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
        smooth_option = "smoothed" if signal_type in self.smoothed_signals else "raw"
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
            self.raised_errors.append(ValidationError(
                ("check_test_vs_reference_avg_changed",
                 checking_date, geo_type, signal_type),
                (mean_stddiff_high, mean_stdabsdiff_high),
                'Average differences in variables by geo_id between recent & reference data '
                + 'seem large --- either large increase '
                + 'tending toward one direction or large mean absolute difference, relative '
                + 'to average values of corresponding variables. For the former check, '
                + 'tolerances for `val` are more restrictive than those for other columns.'))

        self.increment_total_checks()

    def check_duplicate_rows(self, data_df, filename):
        is_duplicate = data_df.duplicated()
        if (any(is_duplicate)):
            duplicate_row_idxs = list(data_df[is_duplicate].index)
            self.raised_warnings.append(ValidationError(
                ("check_duplicate_rows", filename),
                duplicate_row_idxs, 
                "Some rows are duplicated, which may indicate data integrity issues"))
        self.increment_total_checks()

    def validate(self, export_dir):
        """
        Runs all data checks.

        Arguments:
            - export_dir: path to data CSVs

        Returns:
            - None
        """
        frames_list = load_all_files(export_dir, self.start_date, self.end_date)
        self._run_single_file_checks(frames_list)
        all_frames = aggregate_frames(frames_list)
        self._run_combined_file_checks(all_frames)
        self.exit()

    def _run_single_file_checks(self, file_list):
        """
        Perform checks over single-file data sets.

        Parameters
        ----------
        loaded_data: List[Tuple(str, re.match, pd.DataFrame)]
            triples of filenames, filename matches with the geo regex, and the data from the file
        """

        self.check_missing_date_files(file_list)

        # Individual file checks
        # For every daily file, read in and do some basic format and value checks.
        for filename, match, data_df in file_list:
            self.check_df_format(data_df, filename)
            self.check_duplicate_rows(data_df, filename)
            self.check_bad_geo_id_format(
                data_df, filename, match.groupdict()['geo_type'])
            self.check_bad_geo_id_value(
                data_df, filename, match.groupdict()['geo_type'])
            self.check_bad_val(data_df, filename, match.groupdict()['signal'])
            self.check_bad_se(data_df, filename)
            self.check_bad_sample_size(data_df, filename)

    def _run_combined_file_checks(self, all_frames):
        """
        Performs all checks over the combined data set from all files.

        Parameters
        ----------
        all_frames: pd.DataFrame
            combined data from all input files
        """
        # recent_lookbehind: start from the check date and working backward in time,
        # how many days at a time do we want to check for anomalies?
        # Choosing 1 day checks just the daily data.
        recent_lookbehind = timedelta(days=1)

        # semirecent_lookbehind: starting from the check date and working backward
        # in time, how many days do we use to form the reference statistics.
        semirecent_lookbehind = timedelta(days=7)

        # Get list of dates we want to check.
        date_list = [self.start_date + timedelta(days=days)
                     for days in range(self.span_length.days + 1)]

        # Get 14 days prior to the earliest list date
        outlier_lookbehind = timedelta(days=14)

        # Get all expected combinations of geo_type and signal.
        geo_signal_combos = get_geo_signal_combos(self.data_source)

        all_api_df = threaded_api_calls(self.data_source, self.start_date - outlier_lookbehind,
                                        self.end_date, geo_signal_combos)

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

            self.increment_total_checks()

            if geo_sig_df.empty:
                self.raised_errors.append(ValidationError(
                    ("check_missing_geo_sig_combo", geo_type, signal_type),
                    None,
                    "file with geo_type-signal combo does not exist"))
                continue

            max_date = geo_sig_df["time_value"].max()
            self.check_min_allowed_max_date(max_date, geo_type, signal_type)
            self.check_max_allowed_max_date(max_date, geo_type, signal_type)

            # Get relevant reference data from API dictionary.
            api_df_or_error = all_api_df[(geo_type, signal_type)]

            self.increment_total_checks()
            if isinstance(api_df_or_error, APIDataFetchError):
                self.raised_errors.append(api_df_or_error)
                continue

            # Outlier dataframe
            if (signal_type in ["confirmed_7dav_cumulative_num", "confirmed_7dav_incidence_num",
                                "confirmed_cumulative_num", "confirmed_incidence_num",
                                "deaths_7dav_cumulative_num",
                                "deaths_cumulative_num"]):
                earliest_available_date = geo_sig_df["time_value"].min()
                source_df = geo_sig_df.query(
                    'time_value <= @date_list[-1] & time_value >= @date_list[0]')

                # These variables are interpolated into the call to `api_df_or_error.query()`
                # below but pylint doesn't recognize that.
                # pylint: disable=unused-variable
                outlier_start_date = earliest_available_date - outlier_lookbehind
                outlier_end_date = earliest_available_date - timedelta(days=1)
                outlier_api_df = api_df_or_error.query(
                    'time_value <= @outlier_end_date & time_value >= @outlier_start_date')
                # pylint: enable=unused-variable

                self.check_positive_negative_spikes(
                    source_df, outlier_api_df, geo_type, signal_type)

            # Check data from a group of dates against recent (previous 7 days,
            # by default) data from the API.
            for checking_date in date_list:
                recent_cutoff_date = checking_date - \
                    recent_lookbehind + timedelta(days=1)
                recent_df = geo_sig_df.query(
                    'time_value <= @checking_date & time_value >= @recent_cutoff_date')

                self.increment_total_checks()

                if recent_df.empty:
                    self.raised_errors.append(ValidationError(
                        ("check_missing_geo_sig_date_combo",
                         checking_date, geo_type, signal_type),
                        None,
                        "test data for a given checking date-geo type-signal type"
                        + " combination is missing. Source data may be missing"
                        + " for one or more dates"))
                    continue

                # Reference dataframe runs backwards from the recent_cutoff_date
                #
                # These variables are interpolated into the call to `api_df_or_error.query()`
                # below but pylint doesn't recognize that.
                # pylint: disable=unused-variable
                reference_start_date = recent_cutoff_date - \
                    min(semirecent_lookbehind, self.max_check_lookbehind) - \
                    timedelta(days=1)
                reference_end_date = recent_cutoff_date - timedelta(days=1)
                # pylint: enable=unused-variable

                # Subset API data to relevant range of dates.
                reference_api_df = api_df_or_error.query(
                    "time_value >= @reference_start_date & time_value <= @reference_end_date")

                self.increment_total_checks()

                if reference_api_df.empty:
                    self.raised_errors.append(ValidationError(
                        ("empty_reference_data",
                         checking_date, geo_type, signal_type), None,
                        "reference data is empty; comparative checks could not be performed"))
                    continue

                self.check_max_date_vs_reference(
                    recent_df, reference_api_df, checking_date, geo_type, signal_type)

                if self.sanity_check_rows_per_day:
                    self.check_rapid_change_num_rows(
                        recent_df, reference_api_df, checking_date, geo_type, signal_type)

                if self.sanity_check_value_diffs:
                    self.check_avg_val_vs_reference(
                        recent_df, reference_api_df, checking_date, geo_type, signal_type)

            # Keeps script from checking all files in a test run.
            kroc += 1
            if self.test_mode and kroc == 2:
                break

    def exit(self):
        """
        If any not-suppressed exceptions were raised, print and exit with non-zero status.
        """
        suppressed_counter = 0
        subset_raised_errors = []

        for val_error in self.raised_errors:
            # Convert any dates in check_data_id to strings for the purpose of comparing
            # to manually suppressed errors.
            raised_check_id = tuple([
                item.strftime("%Y-%m-%d") if isinstance(item, (date, datetime))
                else item for item in val_error.check_data_id])

            if raised_check_id not in self.suppressed_errors:
                subset_raised_errors.append(val_error)
            else:
                self.suppressed_errors.remove(raised_check_id)
                suppressed_counter += 1

        print(self.total_checks, "checks run")
        print(len(subset_raised_errors), "checks failed")
        print(suppressed_counter, "checks suppressed")
        print(len(self.raised_warnings), "warnings")

        for message in subset_raised_errors:
            print(message)
        for message in self.raised_warnings:
            print(message)

        if len(subset_raised_errors) != 0:
            sys.exit(1)
        else:
            sys.exit(0)
