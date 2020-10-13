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
install the common DELPHI tools, install the indicator module and its
dependencies, and then install the validator module and its
dependencies to the virtual environment.

To do this, navigate to the main directory of the indicator of interest and run the following code:

```
python -m venv env
source env/bin/activate
pip install ../_delphi_utils_python/.
pip install .
pip install ../validator
```

All of the user-changable parameters are stored in the `validation` field of the indicator's `params.json` file. If `params.json` does not already include a `validation` field, please copy that provided in this module's `params.json.template`. Working defaults are provided for all but `data_source`, `span_length`, and `end_date`.

The `data_source` should match the [formatting](https://cmu-delphi.github.io/delphi-epidata/api/covidcast_signals.html) as used in COVIDcast API calls. `end_date` specifies the last date to be checked; if set to "latest", `end_date` will always be the current date. `span_length` specifies the number of days before the `end_date` to check. `span_length` should be long enough to contain all recent source data that is still in the process of being updated, for example, if the data source of interest has a 2-week lag before all reports are in for a given date, `scan_length` should be 14 days.

To execute the module and validate source data (by default, in `receiving`), run the indicator to generate data files, then run
the validator, as follows:

```
env/bin/python -m delphi_INDICATORNAME
env/bin/python -m delphi_validator
```

Once you are finished with the code, you can deactivate the virtual environment
and (optionally) remove the environment itself.

```
deactivate
rm -r env
```

## Testing the code

To test the code, please create a new virtual environment in the main module directory using the following procedure, similar to above:

```
python -m venv env
source env/bin/activate
pip install ../_delphi_utils_python/.
pip install .
```

To do a static test of the code style, it is recommended to run **pylint** on
the module. To do this, run the following from the main module directory:

```
env/bin/pylint delphi_validator
```

The most aggressive checks are turned off; only relatively important issues
should be raised and they should be manually checked (or better, fixed).

Unit tests are also included in the module. To execute these, run the following command from this directory:

```
(cd tests && ../env/bin/pytest --cov=delphi_validator --cov-report=term-missing)
```

The output will show the number of unit tests that passed and failed, along with the percentage of code covered by the tests. None of the tests should fail and the code lines that are not covered by unit tests should be small and should not include critical sub-routines.


## Code tour

* run.py: sends params.json fields to and runs the validation process
* datafetcher.py: methods for loading source and API data
* validate.py: methods for validating data. Includes the individual check methods and supporting functions.
* errors.py: custom errors


## Adding checks

To add a new validation check, define the check as a `Validator` class method in `validate.py`. Each check should append a descriptive error message to the `raised` attribute if triggered. All checks should allow the user to override exception raising for a specific file using the `exception_override` setting in `params.json`.

This features requires that the `check_data_id` defined for an error uniquely identifies that combination of check and test data. This usually takes the form of a tuple of strings with the check method and test identifier, and test data filename or date, geo type, and signal name.

Add the newly defined check to the `validate()` method to be executed. It should go in one of three sections:

* data sanity checks where a data file is compared against static format settings,
* data trend and value checks where a set of data is compared against recent API data, from the previous few days,
* data trend and value checks where a set of data is compared against long term API data, from a few months ago