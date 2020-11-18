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
- Clck the `Create New App Token` button
- Fill in `Application Name` and `Description` (You can just use NCHS_Mortality
  for both) and click `Save`
- Copy the `App Token`

## Running the Indicator

The indicator is run by directly executing the Python module contained in this
directory. The safest way to do this is to create a virtual environment,
installed the common DELPHI tools, and then install the module and its
dependencies. To do this, run the following code from this directory:

```
make install
```

This command will install the package in editable mode, so you can make changes that
will automatically propagate to the installed package. 

All of the user-changable parameters are stored in `params.json`. (NOTE: In
production we specify `"export_start_date": "latest",`). To execute the module
and produce the output datasets (by default, in `receiving`), run the following.
Fill in the `token` that you created.

```
env/bin/python -m delphi_nchs_mortality
```

If you want to enter the virtual environment in your shell, 
you can run `source env/bin/activate`. Run `deactivate` to leave the virtual environment. 

Once you are finished, you can remove the virtual environment and 
params file with the following:

```
make clean
```

## Testing the code

To run static tests of the code style, run the following command:

```
make lint
```

Unit tests are also included in the module. To execute these, run the following
command from this directory:

```
make test
```

To run individual tests, run the following:

```
(cd tests && ../env/bin/pytest <your_test>.py --cov=delphi_nchs_mortality --cov-report=term-missing)
```

The output will show the number of unit tests that passed and failed, along
with the percentage of code covered by the tests. 

None of the linting or unit tests should fail, and the code lines that are not covered by unit tests should be small and
should not include critical sub-routines. 
