# -*- coding: utf-8 -*-
"""
Tools to validate CSV source data, including various check methods.
"""
import sys
import re
import math
import threading
from os.path import join
from datetime import date, datetime, timedelta
import pandas as pd
from .errors import ValidationError, APIDataFetchError
import time
from .datafetcher import filename_regex, \
    read_filenames, load_csv, get_geo_signal_combos, \
    fetch_api_reference

# Recognized geo types.
geo_regex_dict = {
    'county': '^\d{5}$',
    'hrr': '^\d{1,3}$',
    'msa': '^\d{5}$',
    'dma': '^\d{3}$',
    'state': '^[a-zA-Z]{2}$',
    'national': '^[a-zA-Z]{2}$'
}


def relative_difference_by_min(x, y):
    """
    Calculate relative difference between two numbers.
    """
    return (x - y) / min(x, y)


def make_date_filter(start_date, end_date):
    """
    Create a function to return a boolean of whether a filename of appropriate
    format contains a date within (inclusive) the specified date range.

    Arguments:
        - start_date: datetime date object
        - end_date: datetime date object

    Returns:
        - Custom function object
    """
    # Convert dates from datetime format to int.
    start_code = int(start_date.strftime("%Y%m%d"))
    end_code = int(end_date.strftime("%Y%m%d"))

    def custom_date_filter(match):
        """
        Return a boolean of whether a filename of appropriate format contains a date
        within the specified date range.

        Arguments:
            - match: regex match object based on filename_regex applied to a filename str

        Returns:
            - boolean
        """
        # If regex match doesn't exist, current filename is not an appropriately
        # formatted source data file.
        if not match:
            return False

        # Convert date found in CSV name to int.
        code = int(match.groupdict()['date'])

        # Return boolean True if current file date "code" is within the defined date range.
        return start_code <= code <= end_code

    return custom_date_filter


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
        # Get user settings from params or if not provided, set default.
        self.data_source = params['data_source']

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

    def increment_total_checks(self):
        """ Add 1 to total_checks counter """
        self.total_checks += 1

    def check_missing_date_files(self, daily_filenames):
        """
        Check for missing dates between the specified start and end dates.

        Arguments:
            - daily_filenames: list of tuples, each containing CSV source data filename
            and the regex match object corresponding to filename_regex.

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

    def check_settings(self):
        """
        Perform some automated format & sanity checks of parameters.

        Arguments:
            - None

        Returns:
            - None
        """
        if not isinstance(self.max_check_lookbehind, timedelta):
            self.raised_errors.append(ValidationError(
                ("check_type_max_check_lookbehind"),
                self.max_check_lookbehind,
                "max_check_lookbehind must be of type datetime.timedelta"))

        self.increment_total_checks()

        if not isinstance(self.generation_date, date):
            self.raised_errors.append(ValidationError(
                ("check_type_generation_date"), self.generation_date,
                "generation_date must be a datetime.date type"))

        self.increment_total_checks()

        if self.generation_date > date.today():
            self.raised_errors.append(ValidationError(
                ("check_future_generation_date"), self.generation_date,
                "generation_date must not be in the future"))

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
        pattern_found = filename_regex.match(nameformat)
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

    def check_bad_geo_id(self, df_to_test, nameformat, geo_type):
        """
        Check validity of geo type and values, according to regex pattern.

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
                df_to_test["geo_id"] = [geo.zfill(fill_len[geo_type])
                                        for geo in df_to_test["geo_id"]]

            expected_geos = [geo[0] for geo in df_to_test['geo_id'].str.findall(
                geo_regex) if len(geo) > 0]

            unexpected_geos = {geo for geo in set(
                df_to_test['geo_id']) if geo not in expected_geos}

            if len(unexpected_geos) > 0:
                self.raised_errors.append(ValidationError(
                    ("check_geo_id_format", nameformat),
                    unexpected_geos, "Non-conforming geo_ids found"))

        if geo_type not in geo_regex_dict:
            self.raised_errors.append(ValidationError(
                ("check_geo_type", nameformat),
                geo_type, "Unrecognized geo type"))
        else:
            find_all_unexpected_geo_ids(
                df_to_test, geo_regex_dict[geo_type], geo_type)

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



    def data_corrections(self, source_df, api_frames, geo, sig, checking_date):
        """
        Adapt Dan's/Balasubramanian's corrections package to Python (only consider spikes) : https://github.com/cmu-delphi/covidcast-forecast/tree/dev/corrections/data_corrections

        Arguments: 
            - df_to_test: pandas dataframe of CSV source data
            - df_to_reference: pandas dataframe of reference data, either from the
            COVIDcast API or semirecent data
            - checking_date: datetime date
            - geo: str; geo type name (county, msa, hrr, state) as in the CSV name
            - sig: str; signal name as in the CSV name

        """
        # Check inputs: 
        if not (geo == "state" or geo =="county"):
            self.increment_total_checks()
            self.raised_errors.append(ValidationError(
                        ("data_corrections_sig", geo, sig),
                        None,
                        "geo_type should be one of 'state' or 'county'!"))

            return 

        source_df.to_csv("source" +  str(checking_date.date()) +  ".csv")
        # Combine all possible frames so that the rolling window calculations make sense, even if the before or after source frame is None
        all_frames = pd.concat([api_frames, source_df]).drop_duplicates().sort_values(by=['time_value'])

        # Tuned Variables from Dan's Code 
        size_cut = 20
        sig_cut = 3
        sig_consec = 2.25

        # A function mapped to each row to determine outliers based on fstat and ststat values 
        def outlier_flag(frame):
            if (abs(frame["val"]) > size_cut) and not (pd.isna(frame["ststat"])) and (frame["ststat"] > sig_cut):
                return 1
            if (abs(frame["val"]) > size_cut) and (pd.isna(frame["ststat"])) and not (pd.isna(frame["ftstat"])) and (frame["ftstat"] > sig_cut):
                return 1
            if (frame["val"] < -size_cut) and not (pd.isna(frame["ststat"])) and not (pd.isna(frame["ftstat"])):
                return 1
            return 0

        # Calculate ftstat and ststat values for the rolling windows, group fames by geo region 
        region_group = all_frames.groupby("geo_id")
        window_size = 14
        shift_val = 0
        # Shift the window to match how R calculates rolling windows with even numbers
        if (window_size%2 == 0):
            shift_val = -1

        all_full_frames = []
        for cat, group in region_group:
            rolling_windows = group["val"].rolling(window_size, min_periods=window_size)
            center_windows = group["val"].rolling(window_size, min_periods=window_size, center=True)
            fmean = rolling_windows.mean()
            fmedian = rolling_windows.median()
            smedian = center_windows.median().shift(shift_val)
            fsd = rolling_windows.std()
            ssd = center_windows.std().shift(shift_val)
            vals_modified_f = group["val"] - fmedian.fillna(0)
            vals_modified_s = group["val"] - smedian.fillna(0)
            rolling_windows_f = vals_modified_f.rolling(window_size, min_periods=window_size)
            center_windows_s = vals_modified_s.rolling(window_size, min_periods=window_size, center=True)
            fmad = rolling_windows_f.median()
            smad = center_windows_s.median().shift(shift_val)
            ftstat = abs(vals_modified_f)/fsd
            ststat = abs(vals_modified_s)/ssd
            #print(vals_modified_f)
            group['fmean'] = fmean
            group['fmedian'] = fmedian
            group['smedian'] = smedian
            group['fsd'] = fsd
            group['ssd'] = ssd
            group['fmad'] = fmad
            group['smad'] = smad
            group['ftstat'] = ftstat
            group['ststat'] = ststat 
            all_full_frames.append(group) 

        all_frames = pd.concat(all_full_frames)

        # Determine outliers
        outlier_source_df = all_frames.sort_values(by=['time_value']).copy()
        outlier_source_df["flag"] = outlier_source_df.apply(outlier_flag, axis = 1)
        outlier_group = outlier_source_df.groupby("geo_id")

        outlier_append = pd.DataFrame()
        for cat, group in outlier_group:
            group = group.reset_index()
            for index, row in group.iterrows():
                if row["flag"] == 1:
                    try: 
                        eval_next = group.iloc[index+1, :]
                        if (not pd.isna(eval_next['ststat'])) and (eval_next['ststat'] > sig_consec):
                            eval_next["flag"] == 1
                            outlier_append = outlier_append.append(eval_next, ignore_index=True)

                        if pd.isna(eval_next['ststat']) and  (eval_next['ftstat'] > sig_consec):
                            eval_next["flag"] == 1
                            outlier_append = outlier_append.append(eval_next, ignore_index=True)

                    except: 
                        continue
                    try: 
                        eval_prev = group.iloc[index-1, :]
                        if (not pd.isna(eval_prev['ststat'])) and (eval_prev['ststat'] > sig_consec):
                            eval_prev["flag"] == 1
                            outlier_append = outlier_append.append(eval_prev, ignore_index=True)
                        if pd.isna(eval_prev['ststat']) and  (eval_prev['ftstat'] > sig_consec):
                            eval_prev["flag"] == 1
                            outlier_append = outlier_append.append(eval_prev, ignore_index=True)
                    except: 
                        continue                 

        outlier_append["flag"] = 1
        outliers = outlier_source_df[outlier_source_df["flag"] == 1]
        all_o = pd.concat([outliers, outlier_append]).drop(columns=['index']).sort_values(by=['time_value','geo_id']).drop_duplicates()
        all_o = all_o.reset_index().drop(columns=['index'])
        all_o.to_csv(str(checking_date.date()) + sig + "outliers.csv")
        all_frames.to_csv(str(checking_date.date()) + sig + "all_frames.csv")

        if outliers.shape[0] > 0:
            self.raised_errors.append(ValidationError(
                ("data_corrections_range",
                 (checking_date.date()-timedelta(days=1), checking_date.date()+timedelta(days=1)), geo, sig),
                (outliers),
                'Dates with flagged ouliers based on the previous 30 days of source data available'))
        self.increment_total_checks()


    
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
        raw_thresholds = pd.DataFrame(
            [[1.50, 1.30, 1.80]], columns=classes)
        smoothed_thresholds = raw_thresholds.apply(
            lambda x: x/(math.sqrt(7) * 1.5))

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
        mean_stdabsdiff_high = (
            df_all["mean_stdabsdiff"] > float(thres["mean_stdabsdiff"])).any()

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

    def validate(self, export_dir):
        """
        Runs all data checks.

        Arguments:
            - export_dir: path to data CSVs

        Returns:
            - None
        """

        # Get relevant data file names and info.
        export_files = read_filenames(export_dir)
        date_filter = make_date_filter(self.start_date, self.end_date)
     

        # Make list of tuples of CSV names and regex match objects.
        validate_files = [(f, m) for (f, m) in export_files if date_filter(m)]
        self.check_missing_date_files(validate_files)
        self.check_missing_dates(validate_files)
        self.check_settings()

        all_frames = []

        # Individual file checks
        # For every daily file, read in and do some basic format and value checks.
        for filename, match in validate_files:
            data_df = load_csv(join(export_dir, filename))
            self.check_df_format(data_df, filename)
            self.check_bad_geo_id(
                data_df, filename, match.groupdict()['geo_type'])
            self.check_bad_val(data_df, filename, match.groupdict()['signal'])
            self.check_bad_se(data_df, filename)
            self.check_bad_sample_size(data_df, filename)

            # Get geo_type, date, and signal name as specified by CSV name.
            data_df['geo_type'] = match.groupdict()['geo_type']
            data_df['time_value'] = datetime.strptime(
                match.groupdict()['date'], "%Y%m%d").date()
            data_df['signal'] = match.groupdict()['signal']

            # Add current CSV data to all_frames.
            all_frames.append(data_df)

        all_frames = pd.concat(all_frames)

        # Get list of dates we expect to see in the source data.
        date_slist = all_frames['date'].unique().tolist()
        date_list = list(
            map(lambda x: datetime.strptime(x, '%Y%m%d'), date_slist))
        date_list.sort()

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

        #get 30 days prior to the earliest list date 
        outlier_lookbehind = timedelta(days=30)

        # Get all expected combinations of geo_type and signal.
        geo_signal_combos = get_geo_signal_combos(self.data_source)

        all_api_df = self.threaded_api_calls(
            self.start_date - min(semirecent_lookbehind,
                                  self.max_check_lookbehind),
            self.end_date, geo_signal_combos)

        # Keeps script from checking all files in a test run.
        if self.test_mode:
            kroc = 0


        prev_df = None
        next_df = None
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
            geo_sig_api_df = all_api_df[(geo_type, signal_type)]

            if geo_sig_api_df is None:
                continue

            # Check data from a group of dates against recent (previous 7 days,
            # by default) data from the API.
            for checking_date in date_list:
                recent_cutoff_date = checking_date - \
                    recent_lookbehind + timedelta(days=1)
            # Check data from a group of dates against recent (previous 7 days, by default) and against all 
            # data from the API.
            for index, checking_date in enumerate(date_list):
                recent_cutoff_date = checking_date - recent_lookbehind
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
                reference_start_date = recent_cutoff_date - \
                    min(semirecent_lookbehind, self.max_check_lookbehind) - \
                    timedelta(days=1)
                reference_end_date = recent_cutoff_date - timedelta(days=1)

                # Subset API data to relevant range of dates.
                reference_api_df = geo_sig_api_df.query(
                    "time_value >= @reference_start_date & time_value <= @reference_end_date")

                self.increment_total_checks()

                if reference_api_df.empty:
                    self.raised_errors.append(ValidationError(
                        ("empty_reference_data",
                         checking_date, geo_type, signal_type), None,
                        "reference data is empty; comparative checks could not be performed"))
                    continue


                
                
                # Source frame with the day before and after
                next_cutoff_date = checking_date + recent_lookbehind
                source_prev_next_df = geo_sig_df.query(
                    'time_value <= @next_cutoff_date & time_value >= @recent_cutoff_date')


                earliest_available_date = source_prev_next_df["time_value"].min()
                # Outlier dataframe runs backwards from the checking date, in the future we should reduce the number of api calls
                outlier_start_date = recent_cutoff_date - outlier_lookbehind
                outlier_end_date = earliest_available_date - timedelta(days=1)
                outlier_api_df = fetch_api_reference(
                    self.data_source, outlier_start_date, outlier_end_date, geo, sig)

                print(outlier_start_date, outlier_end_date, recent_cutoff_date, next_cutoff_date, earliest_available_date)
                self.data_corrections(source_prev_next_df, outlier_api_df, geo, sig, checking_date)
                prev_df = recent_df




                self.check_max_date_vs_reference(
                    recent_df, reference_api_df, checking_date, geo_type, signal_type)

                if self.sanity_check_rows_per_day:
                    self.check_rapid_change_num_rows(
                        recent_df, reference_api_df, checking_date, geo_type, signal_type)

                if self.sanity_check_value_diffs:
                    self.check_avg_val_vs_reference(
                        recent_df, reference_api_df, checking_date, geo_type, signal_type)

            

            # Keeps script from checking all files in a test run. 
            if self.test_mode:
                kroc += 1
                if kroc == 2:
                    break




        self.exit()

    def get_one_api_df(self, min_date, max_date,
                       geo_type, signal_type,
                       api_semaphore, dict_lock, output_dict):
        """
        Pull API data for a single geo type-signal combination. Raises
        error if data couldn't be retrieved. Saves data to data dict.
        """
        api_semaphore.acquire()

        # Pull reference data from API for all dates.
        try:
            geo_sig_api_df = fetch_api_reference(
                self.data_source, min_date, max_date, geo_type, signal_type)

        except APIDataFetchError as e:
            self.increment_total_checks()
            self.raised_errors.append(ValidationError(
                ("api_data_fetch_error", geo_type, signal_type), None, e))

            geo_sig_api_df = None

        api_semaphore.release()

        # Use a lock so only one thread can access the dictionary.
        dict_lock.acquire()
        output_dict[(geo_type, signal_type)] = geo_sig_api_df
        dict_lock.release()

    def threaded_api_calls(self, min_date, max_date,
                           geo_signal_combos, n_threads=32):
        """
        Get data from API for all geo-signal combinations in a threaded way
        to save time.
        """
        if n_threads > 32:
            n_threads = 32
            print("Warning: Don't run more than 32 threads at once due "
                  + "to API resource limitations")

        output_dict = dict()
        dict_lock = threading.Lock()
        api_semaphore = threading.Semaphore(value=n_threads)

        thread_objs = [threading.Thread(
            target=self.get_one_api_df, args=(min_date, max_date,
                                              geo_type, signal_type,
                                              api_semaphore,
                                              dict_lock, output_dict)
        ) for geo_type, signal_type in geo_signal_combos]

        # Start all threads.
        for thread in thread_objs:
            thread.start()

        # Wait until all threads are finished.
        for thread in thread_objs:
            thread.join()

        return output_dict

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
