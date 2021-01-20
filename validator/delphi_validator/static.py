"""Static file checks."""
from os.path import join
import re
from datetime import datetime
from dataclasses import dataclass
import pandas as pd
from .datafetcher import FILENAME_REGEX
from .errors import ValidationFailure
from .utils import GEO_REGEX_DICT, TimeWindow

class StaticValidator:
    """Class for validation of static properties of individual datasets."""

    @dataclass
    class Parameters:
        """Configuration parameters."""
        # Place to find the data files
        validator_static_file_dir: str
        # Span of time over which to perform checks
        time_window: TimeWindow
        # Threshold for reporting small sample sizes
        minimum_sample_size: int
        # Whether to report missing standard errors
        missing_se_allowed: bool
        # Whether to report missing sample sizes
        missing_sample_size_allowed: bool

    def __init__(self, params):
        """
        Initialize object and set parameters.

        Arguments:
            - params: dictionary of user settings; if empty, defaults will be used
        """
        global_params = params["global"]
        static_params = params.get("static", dict())

        self.params = self.Parameters(
            validator_static_file_dir = static_params.get('validator_static_file_dir',
                                                             '../validator/static'),
            time_window = TimeWindow.from_params(global_params["end_date"],
                                                 global_params["span_length"]),
            minimum_sample_size = static_params.get('minimum_sample_size', 100),
            missing_se_allowed = static_params.get('missing_se_allowed', False),
            missing_sample_size_allowed = static_params.get('missing_sample_size_allowed', False)
        )


    def validate(self, file_list, report):
        """
        Perform checks over single-file data sets.

        Parameters
        ----------
        loaded_data: List[Tuple(str, re.match, pd.DataFrame)]
            triples of filenames, filename matches with the geo regex, and the data from the file
        report: ValidationReport
            report to which the results of these checks will be added
        """

        self.check_missing_date_files(file_list, report)

        # Individual file checks
        # For every daily file, read in and do some basic format and value checks.
        for filename, match, data_df in file_list:
            self.check_df_format(data_df, filename, report)
            self.check_duplicate_rows(data_df, filename, report)
            self.check_bad_geo_id_format(
                data_df, filename, match.groupdict()['geo_type'], report)
            self.check_bad_geo_id_value(
                data_df, filename, match.groupdict()['geo_type'], report)
            self.check_bad_val(data_df, filename, match.groupdict()['signal'], report)
            self.check_bad_se(data_df, filename, report)
            self.check_bad_sample_size(data_df, filename, report)


    def check_missing_date_files(self, daily_filenames, report):
        """
        Check for missing dates between the specified start and end dates.

        Arguments:
            - daily_filenames: List[Tuple(str, re.match, pd.DataFrame)]
                triples of filenames, filename matches with the geo regex, and the data from the
                file
            - report: ValidationReport; report where results are added

        Returns:
            - None
        """
        # Create set of all dates seen in CSV names.
        unique_dates = {datetime.strptime(
            daily_filename[0][0:8], '%Y%m%d').date() for daily_filename in daily_filenames}

        # Diff expected and observed dates.
        check_dateholes = list(set(self.params.time_window.date_seq).difference(unique_dates))
        check_dateholes.sort()

        if check_dateholes:
            report.add_raised_error(ValidationFailure("check_missing_date_files",
                                                      str(check_dateholes),
                                                      "Missing dates are observed; if these dates "
                                                      "are already in the API they would not be "
                                                      "updated"))

        report.increment_total_checks()

    def check_df_format(self, df_to_test, nameformat, report):
        """
        Check basic format of source data CSV df.

        Arguments:
            - df_to_test: pandas dataframe of a single CSV of source data
            (one day-signal-geo_type combo)
            - nameformat: str CSV name; for example, "20200624_county_smoothed_nohh_cmnty_cli.csv"
            - report: ValidationReport; report where results are added

        Returns:
            - None
        """
        pattern_found = FILENAME_REGEX.match(nameformat)
        if not nameformat or not pattern_found:
            report.add_raised_error(ValidationFailure("check_filename_format",
                                                      nameformat,
                                                      "nameformat not recognized"))

        report.increment_total_checks()

        if not isinstance(df_to_test, pd.DataFrame):
            report.add_raised_error(
                ValidationFailure("check_file_data_format",
                                  nameformat,
                                  f"expected pd.DataFrame but got {type(df_to_test)}."))

        report.increment_total_checks()

    def check_bad_geo_id_value(self, df_to_test, filename, geo_type, report):
        """
        Check for bad geo_id values, by comparing to a list of known values (drawn from
        historical data)

        Arguments:
            - df_to_test: pandas dataframe of CSV source data containing the geo_id column to check
            - geo_type: string from CSV name specifying geo type (state, county, msa, etc.) of data
            - report: ValidationReport; report where results are added
       """
        file_path = join(self.params.validator_static_file_dir, geo_type + '_geo.csv')
        valid_geo_df = pd.read_csv(file_path, dtype={'geo_id': str})
        valid_geos = valid_geo_df['geo_id'].values
        unexpected_geos = [geo for geo in df_to_test['geo_id']
                           if geo.lower() not in valid_geos]
        if len(unexpected_geos) > 0:
            report.add_raised_error(ValidationFailure("check_bad_geo_id_value",
                                                      filename,
                                                      f"Unrecognized geo_ids (not in historical "
                                                      "data) {unexpected_geos}"))
        report.increment_total_checks()
        upper_case_geos = [
            geo for geo in df_to_test['geo_id'] if geo.lower() != geo]
        if len(upper_case_geos) > 0:
            report.add_raised_warning(ValidationFailure("check_geo_id_lowercase",
                                                        filename,
                                                        f"geo_ids {upper_case_geos} contains "
                                                        "uppercase characters. Lowercase is "
                                                        "preferred."))
        report.increment_total_checks()

    def check_bad_geo_id_format(self, df_to_test, nameformat, geo_type, report):
        """
        Check validity of geo_type and format of geo_ids, according to regex pattern.

        Arguments:
            - df_to_test: pandas dataframe of CSV source data
            - geo_type: string from CSV name specifying geo type (state, county, msa, hrr) of data
            - report: ValidationReport; report where results are added

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

                    report.add_raised_warning(
                        ValidationFailure("check_geo_id_type",
                                          nameformat,
                                          "geo_ids saved as floats; strings preferred"))

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
                report.add_raised_error(
                    ValidationFailure("check_geo_id_format",
                                      nameformat,
                                      f"Non-conforming geo_ids {unexpected_geos} found"))

        if geo_type in GEO_REGEX_DICT:
            find_all_unexpected_geo_ids(
                df_to_test, GEO_REGEX_DICT[geo_type], geo_type)
        else:
            report.add_raised_error(
                ValidationFailure("check_geo_type",
                                  nameformat,
                                  f"Unrecognized geo type {geo_type}"))

        report.increment_total_checks()

    def check_bad_val(self, df_to_test, nameformat, signal_type, report):
        """
        Check value field for validity.

        Arguments:
            - df_to_test: pandas dataframe of a single CSV of source data
            - signal_type: string from CSV name specifying signal type (smoothed_cli, etc) of data
            - report: ValidationReport; report where results are added

        Returns:
            - None
        """
        # Determine if signal is a proportion (# of x out of 100k people) or percent
        percent_option = bool('pct' in signal_type)
        proportion_option = bool('prop' in signal_type)

        if percent_option:
            if not df_to_test[(df_to_test['val'] > 100)].empty:
                report.add_raised_error(
                    ValidationFailure("check_val_pct_gt_100",
                                      nameformat,
                                      "val column can't have any cell greater than 100 for "
                                      "percents"))

            report.increment_total_checks()

        if proportion_option:
            if not df_to_test[(df_to_test['val'] > 100000)].empty:
                report.add_raised_error(
                    ValidationFailure("check_val_prop_gt_100k",
                                      nameformat,
                                      "val column can't have any cell greater than 100000 for "
                                      "proportions"))

            report.increment_total_checks()

        if df_to_test['val'].isnull().values.any():
            report.add_raised_error(
                ValidationFailure("check_val_missing",
                                  nameformat,
                                  "val column can't have any cell that is NA"))

        report.increment_total_checks()

        if not df_to_test[(df_to_test['val'] < 0)].empty:
            report.add_raised_error(
                ValidationFailure("check_val_lt_0",
                                  nameformat,
                                  "val column can't have any cell smaller than 0"))

        report.increment_total_checks()

    def check_bad_se(self, df_to_test, nameformat, report):
        """
        Check standard errors for validity.

        Arguments:
            - df_to_test: pandas dataframe of a single CSV of source data
            (one day-signal-geo_type combo)
            - nameformat: str CSV name; for example, "20200624_county_smoothed_nohh_cmnty_cli.csv"
            - report: ValidationReport; report where results are added

        Returns:
            - None
        """
        # Add a new se_upper_limit column.
        df_to_test.eval(
            'se_upper_limit = (val * sample_size + 50)/(sample_size + 1)', inplace=True)

        df_to_test['se'] = df_to_test['se'].round(3)
        df_to_test['se_upper_limit'] = df_to_test['se_upper_limit'].round(3)

        if not self.params.missing_se_allowed:
            # Find rows not in the allowed range for se.
            result = df_to_test.query(
                '~((se > 0) & (se < 50) & (se <= se_upper_limit))')

            if not result.empty:
                report.add_raised_error(
                    ValidationFailure("check_se_not_missing_and_in_range",
                                      nameformat,
                                      "se must be in (0, min(50,val*(1+eps))] and not missing"))

            report.increment_total_checks()

            if df_to_test["se"].isnull().mean() > 0.5:
                report.add_raised_error(
                    ValidationFailure("check_se_many_missing",
                                      nameformat,
                                     'Recent se values are >50% NA'))

            report.increment_total_checks()

        elif self.params.missing_se_allowed:
            result = df_to_test.query(
                '~(se.isnull() | ((se > 0) & (se < 50) & (se <= se_upper_limit)))')

            if not result.empty:
                report.add_raised_error(
                    ValidationFailure("check_se_missing_or_in_range",
                                      nameformat,
                                     "se must be NA or in (0, min(50,val*(1+eps))]"))

            report.increment_total_checks()

        result_jeffreys = df_to_test.query('(val == 0) & (se == 0)')
        result_alt = df_to_test.query('se == 0')

        if not result_jeffreys.empty:
            report.add_raised_error(
                ValidationFailure("check_se_0_when_val_0",
                                  nameformat,
                                  "when signal value is 0, se must be non-zero. please "
                                  + "use Jeffreys correction to generate an appropriate se"
                                  + " (see wikipedia.org/wiki/Binomial_proportion_confidence"
                                  + "_interval#Jeffreys_interval for details)"))
        elif not result_alt.empty:
            report.add_raised_error(
                ValidationFailure("check_se_0", nameformat, "se must be non-zero"))

        report.increment_total_checks()

        # Remove se_upper_limit column.
        df_to_test.drop(columns=["se_upper_limit"])

    def check_bad_sample_size(self, df_to_test, nameformat, report):
        """
        Check sample sizes for validity.

        Arguments:
            - df_to_test: pandas dataframe of a single CSV of source data
            (one day-signal-geo_type combo)
            - nameformat: str CSV name; for example, "20200624_county_smoothed_nohh_cmnty_cli.csv"
            - report: ValidationReport; report where results are added

        Returns:
            - None
        """
        if not self.params.missing_sample_size_allowed:
            if df_to_test['sample_size'].isnull().values.any():
                report.add_raised_error(
                    ValidationFailure("check_n_missing",
                                      nameformat,
                                      "sample_size must not be NA"))

            report.increment_total_checks()

            # Find rows with sample size less than minimum allowed
            result = df_to_test.query(
                '(sample_size < @self.params.minimum_sample_size)')

            if not result.empty:
                report.add_raised_error(
                    ValidationFailure("check_n_gt_min",
                                      nameformat,
                                      f"sample size must be >= {self.params.minimum_sample_size}"))

            report.increment_total_checks()

        elif self.params.missing_sample_size_allowed:
            result = df_to_test.query(
                '~(sample_size.isnull() | (sample_size >= @self.params.minimum_sample_size))')

            if not result.empty:
                report.add_raised_error(
                    ValidationFailure("check_n_missing_or_gt_min",
                                      nameformat,
                                      f"sample size must be NA or >= "\
                                          "{self.params.minimum_sample_size}"))

            report.increment_total_checks()

    def check_duplicate_rows(self, data_df, filename, report):
        """
        Check if any rows are duplicated in a data set.

        Parameters
        ----------
        data_df: pd.DataFrame
            data to check
        filename: str
            name of file from which this data came
        report: ValidationReport
            report where results are added
        """
        is_duplicate = data_df.duplicated()
        if any(is_duplicate):
            report.add_raised_warning(
                ValidationFailure("check_duplicate_rows",
                                  filename,
                                  "Some rows are duplicated, which may indicate data integrity "
                                  "issues"))
        report.increment_total_checks()
