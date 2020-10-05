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

from .datafetcher import filename_regex, \
    read_filenames, load_csv, get_geo_sig_cmbo, \
    read_geo_sig_cmbo_files, fetch_api_reference


# Recognized geo types.
negated_regex_dict = {
    'county': '^(?!\d{5}).*$',
    'hrr': '^(?!\d{1,3}).*$',
    'msa': '^(?!\d{5}).*$',
    'state': '^(?![a-z]{2}).*$',
    'national': '(?!usa).*$'
}


def reldiff_by_min(x, y):
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

    def f(match):
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

    return f


class ValidationError(Exception):
    """ Error raised when validation check fails. """

    def __init__(self, check_data_id, expression, message):
        """
        Arguments:
            - check_data_id: str or tuple/list of str uniquely identifying the
            check that was run and on what data
            - expression: relevant variables to message, e.g., if a date doesn't
            pass a check, provide the date
            - message: str explaining why an error was raised
        """
        self.check_data_id = (check_data_id,) if not isinstance(
            check_data_id, tuple) and not isinstance(check_data_id, list) else tuple(check_data_id)
        self.expression = expression
        self.message = message


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
            - check_vs_working: boolean; check flag
            - suppressed_errors: set of check_data_ids used to identify error messages to ignore
            - raised_errors: list to append errors to as they are raised
        """
        # Get user settings from params or if not provided, set default.
        self.data_source = params['data_source']
        self.start_date = datetime.date(
            datetime.strptime(params['start_date'], '%Y-%m-%d'))
        self.end_date = datetime.date(
            datetime.strptime(params['end_date'], '%Y-%m-%d'))
        self.generation_date = params.get('generation_date', date.today())

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
        # TODO: use for something... See https://github.com/cmu-delphi/covid-19/blob/fb-survey/facebook/prepare-extracts/covidalert-io-funs.R#L439
        self.check_vs_working = params.get('check_vs_working', True)

        self.suppressed_errors = {(item,) if not isinstance(item, tuple) and not isinstance(
            item, list) else tuple(item) for item in params.get('suppressed_errors', [])}

        self.raised_errors = []

    def check_missing_dates(self, daily_filenames):
        """
        Check for missing dates between the specified start and end dates.

        Arguments:
            - daily_filenames: list of CSV source data filenames.

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

        if not isinstance(self.generation_date, date):
            self.raised_errors.append(ValidationError(
                ("check_type_generation_date"), self.generation_date,
                "generation_date must be a datetime.date type"))

        if self.generation_date > date.today():
            self.raised_errors.append(ValidationError(
                ("check_future_generation_date"), self.generation_date,
                "generation_date must not be in the future"))

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

        if not isinstance(df_to_test, pd.DataFrame):
            self.raised_errors.append(ValidationError(
                ("check_file_data_format", nameformat),
                type(df_to_test), 'df_to_test must be a pandas dataframe.'))

    def check_bad_geo_id(self, df_to_test, nameformat, geo_type):
        """
        Check validity of geo type and values, according to regex pattern.

        Arguments:
            - df_to_test: pandas dataframe of CSV source data
            - geo_type: string from CSV name specifying geo type (state, county, msa, hrr) of data

        Returns:
            - None
        """
        if geo_type not in negated_regex_dict:
            self.raised_errors.append(ValidationError(
                ("check_geo_type", nameformat),
                geo_type, "Unrecognized geo type"))

        def find_all_unexpected_geo_ids(df_to_test, negated_regex):
            """
            Check if any geo_ids in df_to_test aren't formatted correctly, according
            to the geo type dictionary negated_regex_dict.
            """
            unexpected_geos = [ugeo[0] for ugeo in df_to_test['geo_id'].str.findall(
                negated_regex) if len(ugeo) > 0]
            if len(unexpected_geos) > 0:
                self.raised_errors.append(ValidationError(
                    ("check_geo_id_format", nameformat),
                    unexpected_geos, "Non-conforming geo_ids found"))

        find_all_unexpected_geo_ids(df_to_test, negated_regex_dict[geo_type])

    def check_bad_val(self, df_to_test, nameformat, signal_type):
        """
        Check value field for validity.

        Arguments:
            - df_to_test: pandas dataframe of a single CSV of source data
            - signal_type: string from CSV name specifying signal type (smoothed_cli, etc) of data

        Returns:
            - None
        """
        # Determine if signal is a proportion or percent
        percent_option = bool('pct' in signal_type)
        proportion_option = bool('pct' in signal_type)

        if percent_option:
            if not df_to_test[(df_to_test['val'] > 100)].empty:
                self.raised_errors.append(ValidationError(
                    ("check_val_pct_gt_100", nameformat),
                    df_to_test[(df_to_test['val'] > 100)],
                    "val column can't have any cell greater than 100 for percents"))

        if proportion_option:
            if not df_to_test[(df_to_test['val'] > 100000)].empty:
                self.raised_errors.append(ValidationError(
                    ("check_val_prop_gt_100k", nameformat),
                    df_to_test[(df_to_test['val'] > 100000)],
                    "val column can't have any cell greater than 100000 for nameformat"))

        if df_to_test['val'].isnull().values.any():
            self.raised_errors.append(ValidationError(
                ("check_val_missing", nameformat),
                None, "val column can't have any cell that is NA"))

        if not df_to_test[(df_to_test['val'] < 0)].empty:
            self.raised_errors.append(ValidationError(
                ("check_val_lt_0", nameformat),
                df_to_test[(df_to_test['val'] < 0)],
                "val column can't have any cell smaller than 0"))

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
            if df_to_test['se'].isnull().values.any():
                self.raised_errors.append(ValidationError(
                    ("check_se_missing", nameformat),
                    None, "se must not be NA"))

            # Find rows not in the allowed range for se.
            result = df_to_test.query(
                '~((se > 0) & (se < 50) & (se <= se_upper_limit))')

            if not result.empty:
                self.raised_errors.append(ValidationError(
                    ("check_se_in_range", nameformat),
                    result, "se must be in (0, min(50,val*(1+eps))]"))

            if df_to_test["se"].isnull().mean() > 0.5:
                self.raised_errors.append(ValidationError(
                    ("check_se_many_missing", nameformat),
                    None, 'Recent se values are >50% NA'))

        elif self.missing_se_allowed:
            result = df_to_test.query(
                '~(se.isnull() | ((se > 0) & (se < 50) & (se <= se_upper_limit)))')

            if not result.empty:
                self.raised_errors.append(ValidationError(
                    ("check_se_missing_or_in_range", nameformat),
                    result, "se must be NA or in (0, min(50,val*(1+eps))]"))

        result_jeffreys = df_to_test.query('(val == 0) & (se == 0)')
        result_alt = df_to_test.query('se == 0')

        if not result_jeffreys.empty:
            self.raised_errors.append(ValidationError(
                ("check_se_0_when_val_0", nameformat),
                None,
                "when signal value is 0, se must be non-zero. please "
                + "use Jeffreys correction to generate an appropriate se"))
        elif not result_alt.empty:
            self.raised_errors.append(ValidationError(
                ("check_se_0", nameformat),
                result_alt, "se must be non-zero"))

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

            # Find rows with sample size less than minimum allowed
            result = df_to_test.query(
                '(sample_size < @self.minimum_sample_size)')

            if not result.empty:
                self.raised_errors.append(ValidationError(
                    ("check_n_gt_min", nameformat),
                    result, "sample size must be >= {self.minimum_sample_size}"))

        elif self.missing_sample_size_allowed:
            result = df_to_test.query(
                '~(sample_size.isnull() | (sample_size >= @self.minimum_sample_size))')

            if not result.empty:
                self.raised_errors.append(ValidationError(
                    ("check_n_missing_or_gt_min", nameformat),
                    result,
                    "sample size must be NA or >= {self.minimum_sample_size}"))

    def check_min_allowed_max_date(self, max_date, weighted_option, geo, sig):
        """
        Check if time since data was generated is reasonable or too long ago.

        Arguments:
            - max_date: date of most recent data to be validated; datetime format.
            - weighted_option: str; selects the "reasonable" threshold based on signal name
            - geo: str; geo type name (county, msa, hrr, state) as in the CSV name
            - sig: str; signal name as in the CSV name

        Returns:
            - None
        """
        switcher = {
            'unweighted': timedelta(days=1),
            'weighted': timedelta(days=4)
        }
        # Get the setting from switcher dictionary
        thres = switcher.get(
            weighted_option, lambda: "Invalid weighting option")

        if max_date < self.generation_date - thres:
            self.raised_errors.append(ValidationError(
                ("check_min_max_date", geo, sig),
                max_date.date(),
                "most recent date of generated file seems too long ago"))

    def check_max_allowed_max_date(self, max_date, geo, sig):
        """
        Check if time since data was generated is reasonable or too recent.

        Arguments:
            - max_date: date of most recent data to be validated; datetime format.
            - geo: str; geo type name (county, msa, hrr, state) as in the CSV name
            - sig: str; signal name as in the CSV name

        Returns:
            - None
        """
        if max_date > self.generation_date - timedelta(days=1):
            self.raised_errors.append(ValidationError(
                ("check_max_max_date", geo, sig),
                max_date.date(),
                "most recent date of generated file seems too recent"))

    def check_max_date_vs_reference(self, df_to_test, df_to_reference, checking_date, geo, sig):
        """
        Check if reference data is more recent than test data.

        Arguments:
            - df_to_test: pandas dataframe of a single CSV of source data
            (one day-signal-geo_type combo)
            - df_to_reference: pandas dataframe of reference data, either from the
            COVIDcast API or semirecent data
            - geo: str; geo type name (county, msa, hrr, state) as in the CSV name
            - sig: str; signal name as in the CSV name

        Returns:
            - None
        """
        if df_to_test["time_value"].max() < df_to_reference["time_value"].max():
            self.raised_errors.append(ValidationError(
                ("check_max_date_vs_reference", checking_date.date(), geo, sig),
                (df_to_test["time_value"].max(),
                 df_to_reference["time_value"].max()),
                'reference df has days beyond the max date in the =df_to_test=; ' +
                'checks are not constructed to handle this case, and this situation ' +
                'may indicate that something locally is out of date, or, if the local ' +
                'working files have already been compared against the reference, ' +
                'that there is a bug somewhere'))

    def check_rapid_change(self, df_to_test, df_to_reference, checking_date, geo, sig):
        """
        Compare number of obervations per day in test dataframe vs reference dataframe.

        Arguments:
            - df_to_test: pandas dataframe of CSV source data
            - df_to_reference: pandas dataframe of reference data, either from the
            COVIDcast API or semirecent data
            - checking_date: datetime date
            - geo: str; geo type name (county, msa, hrr, state) as in the CSV name
            - sig: str; signal name as in the CSV name

        Returns:
            - None
        """
        test_rows_per_reporting_day = df_to_test[df_to_test['time_value']
                                                 == checking_date].shape[0]
        reference_rows_per_reporting_day = df_to_reference.shape[0] / len(
            set(df_to_reference["time_value"]))

        if abs(reldiff_by_min(
                test_rows_per_reporting_day,
                reference_rows_per_reporting_day)) > 0.35:
            self.raised_errors.append(ValidationError(
                ("check_rapid_change_num_rows", checking_date.date(), geo, sig),
                (test_rows_per_reporting_day, reference_rows_per_reporting_day),
                "Number of rows per day (-with-any-rows) seems to have changed " +
                "rapidly (reference vs test data)"))

    def check_avg_val_diffs(self,
                            df_to_test, df_to_reference,
                            smooth_option,
                            checking_date,
                            geo, sig):
        """
        Compare average values for each variable in test dataframe vs reference dataframe.

        Arguments:
            - df_to_test: pandas dataframe of CSV source data
            - df_to_reference: pandas dataframe of reference data, either from the
            COVIDcast API or semirecent data
            - smooth_option: "raw" or "smoothed", choosen according to smoothing of signal
            (e.g. 7dav is "smoothed")
            - geo: str; geo type name (county, msa, hrr, state) as in the CSV name
            - sig: str; signal name as in the CSV name

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
        df_all = pd.melt(
            df_all, id_vars=["geo_id", "type"], value_vars=["val", "se", "sample_size"]
        ).pivot(index=("geo_id", "variable"), columns="type", values="value"
                ).reset_index(("geo_id", "variable")
                              ).dropna(
        ).assign(
            type_diff=lambda x: x["test"] - x["reference"],
            abs_type_diff=lambda x: abs(x["type_diff"])
        ).groupby("variable", as_index=False
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
        raw_thresholds = pd.DataFrame([0.50, 0.30, 0.80], classes).T
        smoothed_thresholds = raw_thresholds.apply(
            lambda x: x/(math.sqrt(7) * 1.5))

        switcher = {
            'raw': raw_thresholds,
            'smoothed': smoothed_thresholds,
        }

        # Get the selected thresholds from switcher dictionary
        thres = switcher.get(smooth_option, lambda: "Invalid smoothing option")

        # Check if the calculated mean differences are high compared to the thresholds.
        mean_stddiff_high = (abs(df_all["mean_stddiff"]) > thres["mean_stddiff"]).bool() or (
            (df_all["variable"] == "val").bool() and (
                abs(df_all["mean_stddiff"]) > thres["val_mean_stddiff"]).bool())
        mean_stdabsdiff_high = (
            df_all["mean_stdabsdiff"] > thres["mean_stdabsdiff"]).bool()

        if mean_stddiff_high or mean_stdabsdiff_high:
            self.raised_errors.append(ValidationError(
                ("check_test_vs_reference_avg_changed",
                 checking_date.date(), geo, sig),
                (mean_stddiff_high, mean_stdabsdiff_high),
                'Average differences in variables by geo_id between recent & reference data '
                + '(either semirecent or from API) seem large --- either large increase '
                + 'tending toward one direction or large mean absolute difference, relative '
                + 'to average values of corresponding variables.  For the former check, '
                + 'tolerances for `val` are more restrictive than those for other columns.'))

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

        # Get all expected combinations of geo_type and signal.
        geo_sig_cmbo = get_geo_sig_cmbo(self.data_source)

        self.check_missing_dates(validate_files)
        self.check_settings()

        all_frames = []

        # Individual file checks
        # For every daily file, read in and do some basic format and value checks.
        for filename, match in validate_files:
            df = load_csv(join(export_dir, filename))

            self.check_df_format(df, filename)
            self.check_bad_geo_id(df, filename, match.groupdict()['geo_type'])
            self.check_bad_val(df, filename, match.groupdict()['signal'])
            self.check_bad_se(df, filename)
            self.check_bad_sample_size(df, filename)

            # Get geo_type, date, and signal name as specified by CSV name.
            df['geo_type'] = match.groupdict()['geo_type']
            df['date'] = match.groupdict()['date']
            df['signal'] = match.groupdict()['signal']

            # Add current CSV data to all_frames.
            all_frames.append(df)

        # TODO: Multi-indexed dataframe for a given (signal, geo_type)
        all_frames = pd.concat(all_frames)

        # Get list of dates we expect to see in all the CSV data.
        date_slist = all_frames['date'].unique().tolist()
        date_list = list(
            map(lambda x: datetime.strptime(x, '%Y%m%d'), date_slist))

        # recent_lookbehind: start from the check date and working backward in time,
        # how many days do we want to check for anomalies?
        # Choosing 1 day checks just the daily data.
        recent_lookbehind = timedelta(days=1)

        # semirecent_lookbehind: starting from the check date and working backward
        # in time, how many days do we use to form the reference statistics.
        semirecent_lookbehind = timedelta(days=7)

        smooth_option_regex = re.compile(r'([^_]+)')

        # TODO: Remove for actual version
        kroc = 0

        # TODO: Improve efficiency by grouping all_frames by geo and sig instead
        # of reading data in again via read_geo_sig_cmbo_files().

        # Comparison checks
        # Run checks for recent dates in each geo-sig combo vs semirecent (last week) API data.
        for geo_sig_df, geo, sig in read_geo_sig_cmbo_files(
                geo_sig_cmbo,
                export_dir,
                [name_match_pair[0] for name_match_pair in validate_files],
                date_slist):

            m = smooth_option_regex.match(sig)
            smooth_option = m.group(1)

            if smooth_option not in ('raw', 'smoothed'):
                smooth_option = 'smoothed' if '7dav' in sig or 'smoothed' in sig else 'raw'

            weight_option = 'weighted' if 'wili' in sig or 'wcli' in sig else 'unweighted'

            print("Printing geo_sig_df scenes:", geo_sig_df.shape)
            print(geo_sig_df)

            max_date = geo_sig_df["time_value"].max()
            self.check_min_allowed_max_date(max_date, weight_option, geo, sig)
            self.check_max_allowed_max_date(max_date, geo, sig)

            # TODO: Check to see, if this date is in the API, if values have been updated
            # and changed significantly.

            # TODO: Compare data against long-ago (3 months?) API data for changes in trends.

            # Check data from a group of dates against recent (previous 7 days, by default)
            # data from the API.
            for checking_date in date_list:
                recent_cutoff_date = checking_date - recent_lookbehind
                recent_df = geo_sig_df.query(
                    'time_value <= @checking_date & time_value >= @recent_cutoff_date')

                # Reference dataframe runs backwards from the checking_date
                reference_start_date = checking_date - \
                    min(semirecent_lookbehind, self.max_check_lookbehind)
                reference_end_date = recent_cutoff_date - timedelta(days=1)
                reference_api_df = fetch_api_reference(
                    self.data_source, reference_start_date, reference_end_date, geo, sig)

                self.check_max_date_vs_reference(
                    recent_df, reference_api_df, checking_date, geo, sig)

                if self.sanity_check_rows_per_day:
                    self.check_rapid_change(
                        recent_df, reference_api_df, checking_date, geo, sig)

                if self.sanity_check_value_diffs:
                    self.check_avg_val_diffs(
                        recent_df, reference_api_df, smooth_option, checking_date, geo, sig)

            # TODO: Remove for actual version
            kroc += 1
            if kroc == 2:
                break

        self.exit()

    def exit(self):
        """
        If any not-suppressed exceptions were raised, print and exit with non-zero status.
        """
        if self.raised_errors:
            suppressed_counter = 0
            subset_raised_errors = []

            for val_error in self.raised_errors:
                raised_check_id = tuple(item.strftime("%Y-%m-%d") if isinstance(
                    item, (date, datetime)) else item for item in val_error.check_data_id)

                if raised_check_id not in self.suppressed_errors:
                    subset_raised_errors.append(val_error)
                else:
                    self.suppressed_errors.remove(raised_check_id)
                    suppressed_counter += 1

            print(len(subset_raised_errors), "messages")
            print(suppressed_counter, "suppressed messages")

            if len(subset_raised_errors) == 0:
                sys.exit(0)
            else:
                for message in subset_raised_errors:
                    print(message)

                sys.exit(1)
        else:
            sys.exit(0)
