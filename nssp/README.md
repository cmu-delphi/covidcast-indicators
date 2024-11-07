# NSSP Emergency Department Visit data

We import the NSSP Emergency Department Visit data, currently only the smoothed concentration, from the CDC website, aggregate to the state and national level from the wastewater sample site level, and export the aggregated data.

There are 2 sources we grab data from for nssp:
- Primary source: https://data.cdc.gov/Public-Health-Surveillance/NSSP-Emergency-Department-Visit-Trajectories-by-St/rdmq-nq56/data_preview
- Secondary source: https://data.cdc.gov/Public-Health-Surveillance/2023-Respiratory-Virus-Response-NSSP-Emergency-Dep/7mra-9cq9/data_preview

For details see the `DETAILS.md` file in this directory.

## Create a MyAppToken
`MyAppToken` is required when fetching data from SODA Consumer API 
(https://dev.socrata.com/foundry/data.cdc.gov/r8kw-7aab). Follow the 
steps below to create a MyAppToken.
- Click the `Sign up for an app token` button in the linked website
- Sign In or Sign Up with Socrata ID
- Click the `Create New App Token` button
- Fill in `Application Name` and `Description` (You can just use delphi_wastewater
  for both) and click `Save`
- Copy the `App Token`


## Running the Indicator

The indicator is run by directly executing the Python module contained in this
directory. The safest way to do this is to create a virtual environment,
installed the common DELPHI tools, and then install the module and its
dependencies. To do this, run the following command from this directory:

```
make install
```

This command will install the package in editable mode, so you can make changes that
will automatically propagate to the installed package. 

All of the user-changable parameters are stored in `params.json`. To execute
the module and produce the output datasets (by default, in `receiving`), run
the following:

```
env/bin/python -m delphi_nssp
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
(cd tests && ../env/bin/pytest <your_test>.py --cov=delphi_NAME --cov-report=term-missing)
```

The output will show the number of unit tests that passed and failed, along
with the percentage of code covered by the tests. 

None of the linting or unit tests should fail, and the code lines that are not covered by unit tests should be small and
should not include critical sub-routines. 
