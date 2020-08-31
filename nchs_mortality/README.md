# NCHS Mortality Data

We import the NCHS motality data from CDC website and export
the state-level data as-is. For detailed information see the files 
`DETAILS.md` contained in this directory.

## Create a MyAppToken
`MyAppToken` is required when fetching data from SODA Consumer API 
(https://dev.socrata.com/foundry/data.cdc.gov/r8kw-7aab). Follow the 
steps below to create a MyAppToken.
- Click the `Sign up for an app toekn` buttom in the linked website
- Sign In or Sign Up with Socrata ID
- Clck the `Create New App Token` buttom
- Fill in `Application Name` and `Description` (You can just use NCHS_Mortality
  for both) and click `Save`
- Copy the `App Token`

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

All of the user-changable parameters are stored in `params.json`. (NOTE: In
production we specify `"export_start_date": "latest",`). To execute the module
and produce the output datasets (by default, in `receiving`), run the following.
Fill in the `token` that you created.

```
env/bin/python -m delphi_nchs_mortality
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
env/bin/pylint delphi_nchs_mortality
```

The most aggressive checks are turned off; only relatively important issues
should be raised and they should be manually checked (or better, fixed).

Unit tests are also included in the module. To execute these, run the following
command from this directory:

```
(cd tests && ../env/bin/pytest --cov=delphi_nchs_mortality --cov-report=term-missing)
```

The output will show the number of unit tests that passed and failed, along
with the percentage of code covered by the tests. None of the tests should
fail and the code lines that are not covered by unit tests should be small and
should not include critical sub-routines.
