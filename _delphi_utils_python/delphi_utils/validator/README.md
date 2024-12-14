# Validator

The validator performs two main tasks:
1) Sanity checks on daily data generated from the pipeline of a specific data
   source.
2) Comparative analysis with recent data from the API
   to detect any anomalies, such as spikes or significant value differences

The validator validates new source data in CSV format against data pulled from the [COVIDcast API](https://cmu-delphi.github.io/delphi-epidata/api/covidcast.html).


## Running the Validator

The validator is run by executing the Python module contained in this
directory from the main directory of the indicator of interest.

The safest way to do this is to create a virtual environment,
and install the common DELPHI tools, including the validator, and the
validator module and its dependencies to the virtual environment.

To do this, navigate to the main directory of the indicator of interest and run the following code:

```
python -m venv env
source env/bin/activate
pip install ../_delphi_utils_python/.
pip install .
```

To execute the module and validate source data (by default, in `receiving`), run the indicator to generate data files, then run
the validator, as follows:

```
env/bin/python -m delphi_INDICATORNAME
env/bin/python -m delphi_utils.validator
```

Once you are finished with the code, you can deactivate the virtual environment
and (optionally) remove the environment itself.

```
deactivate
rm -r env
```

### Using the runner

The validator can also be called using the runner, which performs the entire pipeline:

1. Calling the indicator's `run` module
2. Running the validator on newly obtained CSV files, if validator is specified
3. If validation is unsuccessful, remove or transfer current files in the run
4. Archive files if archiver is specified and there are no validation failures
5. Transfer files to delivery directory if specified in params

The following truth table describes behavior of item (3):

| case | dry_run | validation failure | export dir | delivery dir | v failure dir | action |
| - | - | - | - | - | - | - |
| 1 | true | never | * | * | * | no effect |
| 2 | false | no | * | * | * | no effect |
| 3 | false | yes | X | X | * | throw assertion exception |
| 4 | false | yes | X | None | * | throw assertion exception |
| 5 | false | yes | X | Y != X | None | delete CSV from X |
| 6 | false | yes | X | Y != X | Z | move CSV from X to Z |

### Customization

All of the user-changable parameters are stored in the `validation` field of the indicator's `params.json` file. If `params.json` does not already include a `validation` field, please copy that provided in this module's `params.json.template`.

Please update the follow settings:

* `common`: global validation settings
   * `data_source`: should match the [formatting](https://cmu-delphi.github.io/delphi-epidata/api/covidcast_signals.html) as used in COVIDcast API calls
   * `dry_run`: boolean; `true` prevent system exit with error and ensures that the success() method of the ValidationReport always returns true.
   * `end_date`: specifies the last date to be checked; this can be specified as `YYYY-MM-DD`, `today`, `today-{num}`, or `sunday+{num}`.  `today-num` is interpreted as `num` days before the current date. `sunday+{num}` sets the end date as the most recent day of the week prior to today (as specified by the user). The numeric input represents the day of the week (`sunday+1` denotes Monday, and so on, with 0 or 7 both indicating Sunday). Defaults to the global minimum of `min_expected_lag` days behind today.
   * `max_expected_lag` (default: 10 for all unspecified signals): dictionary of signal name-string pairs specifying the maximum number of days of expected lag (time between event occurrence and when data about that event was published) for that signal. Default values can also be set using 'all' as the key: individually setting keys on top of this will override the default. Values are either numeric or `sunday+{num},{num}`. `sunday+{num},{num}` is used for indicators that update data on a regular weekly basis. The first number denotes the day of the week (with Monday = 1 and so on, and Sunday taking either 0 or 7). The second number denotes the expected lag for the data upon upload date. In other words, if the signal updates weekly on Wednesday's for the past week's data up til Sunday, the correct parameter would be something like `sunday+3,3`.
   * `min_expected_lag` (default: 1 for all unspecified signals): dictionary of signal name-string pairs specifying the minimum number of days of expected lag (time between event occurrence and when data about that event was published) for that signal. Default values can be changed by using the 'all' key. See `max_expected_lag` for further details.
   * `span_length`: specifies the number of days before the `end_date` to check. `span_length` should be long enough to contain all recent source data that is still in the process of being updated (i.e. in the backfill period), for example, if the data source of interest has a 2-week lag before all reports are in for a given date, `span_length` should be 14 days
   * `suppressed_errors`: list of objects specifying errors that have been manually verified as false positives or acceptable deviations from expected.  These errors can be specified with the following variables, where omitted values are interpreted as a wildcard, i.e., not specifying a date applies to all dates:
       * `check_name`:  name of the check, as specified in the validation output
       * `date`:  date in `YYYY-MM-DD` format
       * `geo_type`:  geo resolution of the data
       * `signal`:  name of COVIDcast API signal
   * `test_mode`: boolean; `true` checks only a small number of data files
* `static`: settings for validations that don't require comparison with external COVIDcast API data
   * `minimum_sample_size` (default: 100): threshold for flagging small sample sizes as invalid
   * `missing_se_allowed` (default: False): whether signals with missing standard errors are valid
   * `missing_sample_size_allowed` (default: False): whether signals with missing sample sizes are valid
   * `additional_valid_geo_values` (default: `{}`): map of geo type names to lists of geo values that are not recorded in the GeoMapper but are nonetheless valid for this indicator
* `dynamic`: settings for validations that require comparison with external COVIDcast API data
   * `ref_window_size` (default: 14): number of days over which to look back for comparison 
   * `smoothed_signals`: list of the names of the signals that are smoothed (e.g. 7-day average)


## Testing the code

To test the code, please create a new virtual environment in the main module directory using the following procedure, similar to above:

```
python -m venv env
source env/bin/activate
pip install ../_delphi_utils_python/.
```

To do a static test of the code style, it is recommended to run **pylint** on
the module. To do this, run the following from the main module directory:

```
env/bin/pylint delphi_utils.validator
```

The most aggressive checks are turned off; only relatively important issues
should be raised and they should be manually checked (or better, fixed).

Unit tests are also included in the module. To execute these, run the following command from this directory:

```
(cd tests && ../env/bin/pytest --cov=delphi_utils.validator --cov-report=term-missing)
```

The output will show the number of unit tests that passed and failed, along with the percentage of code covered by the tests. None of the tests should fail and the code lines that are not covered by unit tests should be small and should not include critical sub-routines.


## Code tour

* run.py: sends params.json fields to and runs the validation process
* validate.py: top-level organization of validation
* static.py: methods for validating data that don't require comparisons against external API data
* dynamic.py: methods for validating data that require comparisons against external API data
* datafetcher.py: methods for loading source and API data
* errors.py: custom errors
* report.py: organization and logging of validation outcomes
* utils.py: various helper functions


## Adding checks

To add a new validation check, define the check as a `Validator` class method in `validate.py`. Each check should append a descriptive error message to the `raised` attribute if triggered. All checks should allow the user to override exception raising for a specific file using the `suppressed_errors` setting in `params.json`.

This features requires that the `check_data_id` defined for an error uniquely identifies that combination of check and test data. This usually takes the form of a tuple of strings with the check method and test identifier, and test data filename or date, geo type, and signal name.

Add the newly defined check to the `validate()` method to be executed. It should go in one of three sections:

* data sanity checks where a data file is compared against static format settings,
* data trend and value checks where a set of source data (can be one or several days) is compared against recent API data, from the previous few days,
* data trend and value checks where a set of source data is compared against long term API data, from the last few months
