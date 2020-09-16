# Quidel Test Indicators

## Running the Indicator

The indicator is run by directly executing the Python module contained in this
directory. The safest way to do this is to create a virtual environment,
installed the common DELPHI tools, and then install the module and its
dependencies. To do this, run the following code from this directory:

```
python -m venv env
source env/bin/activate
pip install ../_delphi_utils_python/.
pip install .
```

All of the user-changable parameters are stored in `params.json`. A template is
included as `params.json.template`. At a minimum, you will need to include a
password for the datadrop email account and the email address of the data sender. 
Note that setting `export_end_date` to an empty string will export data through 
today (GMT) minus 5 days for COVID Antigen Tests. (It has not been settled for 
Flu Antigen Tests) Setting `pull_end_date` to an empty string will pull data 
through today (GMT).

Quidel COVID test datasets are received by email from the first available date.
However, the earliest part of Quidel Flu test datasets are stored in MIDAS. The 
default of `pull_start_date` for Quidel Flu test is set to be `2020-05-08`, which 
is the first valid date to pull the data from email. When officially running 
this pipeline to get all the historical data for Quidel Flu test, this pipeline 
needs to be run on MIDAS whith `pull_start_date` for Quidel Flu test set to be 
an arbitrary date earlier than `2020-05-08`.

To execute the module and produce the output datasets (by default, in
`receiving`), run the following:

```
env/bin/python -m delphi_quidel
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
env/bin/pylint delphi_quidel
```

The most aggressive checks are turned off; only relatively important issues
should be raised and they should be manually checked (or better, fixed).

Unit tests are also included in the module. To execute these, run the following
command from this directory:

```
(cd tests && ../env/bin/pytest --cov=delphi_quidel --cov-report=term-missing)
```

The output will show the number of unit tests that passed and failed, along
with the percentage of code covered by the tests. None of the tests should
fail and the code lines that are not covered by unit tests should be small and
should not include critical sub-routines.
