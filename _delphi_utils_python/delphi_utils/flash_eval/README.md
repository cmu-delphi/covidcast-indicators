# FlaSH System

THIS README IS IN PROGRESS

FlaSH is a real-time point-outlier detection system. We add the daily evaluation step to this indicators package (retraining is done offline).

FlaSH produces a list of data points that are unusual or surprising so that stakeholders are aware of points that warrant further inspection. 

The guiding principles for the system are: 
- Flag relevant data points as soon as possible (ideally in an online setting)
- Be aware of the false positive/false negative rates
- Reduce cognitive load on data evaluators 

Types of outliers/changes FlaSH intends to catch are: 
- Out-of-range points 
- Large spikes 
- Points that are interesting for a particular weekday
- Points that are interesting with respect to a particular stream's history
- Points that are interesting with respect to all other streams 
- Change in data reporting schedule
- Changes in health condition [ex: new variant]

## Running FlaSH-eval

First, run the indicator so that there are files for FlaSH to check. 

You can excecute the Python module contained in this
directory from the main directory of the indicator of interest.

The safest way to do this is to create a virtual environment,
and install the common DELPHI tools, including flash, and the
flash module and its dependencies to the virtual environment.

To do this, navigate to the main directory of the indicator of interest and run the following code:

```
python -m venv env
source env/bin/activate
pip install ../_delphi_utils_python/.
pip install .
```

To execute the module run the indicator to generate data files and then run
the flash system , as follows:

```
env/bin/python -m delphi_INDICATORNAME
env/bin/python -m delphi_utils.flash_eval

```

Once you are finished with the code, you can deactivate the virtual environment
and (optionally) remove the environment itself.

```
deactivate
rm -r env
```

### Customization

All of the user-changable parameters are stored in the `flash` field of the indicator's `params.json` file. If `params.json` does not already include a `flash` field, please copy that provided in this module's `params.json.template`.

Please update the follow settings:
- signals: a list of which signals for that indicator go through FlaSH. 

## Testing the code

To test the code, please create a new virtual environment in the main module directory using the following procedure, similar to above:

```
make install
```

To do a static test of the code style, it is recommended to run **pylint** on
the module. To do this, run the following from the main module directory:

```
make lint
```

The most aggressive checks are turned off; only relatively important issues
should be raised and they should be manually checked (or better, fixed).

Unit tests are also included in the module. To execute these, run the following command from this directory:

```
make test
```

or 

```
(cd tests && ../env/bin/pytest test_file.py --cov=delphi_utils --cov-report=term-missing)
```

The output will show the number of unit tests that passed and failed, along with the percentage of code covered by the tests. None of the tests should fail and the code lines that are not covered by unit tests should be small and should not include critical sub-routines.


## Adding checks

To add a new validation check. Each check should append a descriptive error message to the `raised` attribute if triggered. All checks should allow the user to override exception raising for a specific file using the `suppressed_errors` setting in `params.json`.
