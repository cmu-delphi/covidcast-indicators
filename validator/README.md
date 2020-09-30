# Validator

The validator performs two main tasks:
1) Sanity checks on daily data generated from the pipeline of a specific data
   source.
2) Comparative analysis with recent data from the API
   to detect any anomalies, such as spikes or significant value differences

The validator validates new source data against daily data that is already written to disk,
making the execution of the validator independent of the pipeline execution.
This creates the additional advantage of validating against multiple
days of daily data for a better cummulative analysis.


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
pip install -e ../validator
```

All of the user-changable parameters are stored in the `validation` field of the indicator's `params.json` file. If `params.json` does not already include a `validation` field, please copy that provided in this module's `params.json.template`. Working defaults are provided for all but `data_source`, `start_date`, and `end_date`. The `data_source` should match the [formatting](https://cmu-delphi.github.io/delphi-epidata/api/covidcast_signals.html) as used in COVIDcast API calls.

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

To do a static test of the code style, it is recommended to run **pylint** on
the module. To do this, run the following from the main module directory:

```
env/bin/pylint delphi_validator
```

The most aggressive checks are turned off; only relatively important issues
should be raised and they should be manually checked (or better, fixed).


## Code tour

* run.py: sends params.json fields to and runs the validation process
* datafetcher.py: methods for loading source data
* validate.py: methods for validating source data. Includes the individual check functions.
* errors.py: custom validation errors


## Adding checks

To add a new validation check, define the check as a `Validator` class method in `validate.py`. Each check should append a descriptive error message to the `raised` attribute if triggered. All checks should allow the user to override exception raising for a specific file using the `exception_override` setting in `params.json`.

Add the newly defined check to the `validate()` method to be executed. It should go in one of two sections: data sanity checks where a data file is compared against static format settings, or data trend and value checks where a set of data is compared against API data.