# Claims Hospitalizations Indicator

COVID-19 indicator using hospitalizations from claims records.
Makes appropriate date shifts, pads for small counts, and smooths estimates.
Results are written to CSV.

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

All of the user-changable parameters are stored in `params.json`. To execute
the module and produce the output datasets (by default, in `receiving`), run
the following:

```
env/bin/python -m delphi_claims_hosp
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
(cd tests && ../env/bin/pytest <your_test>.py --cov=delphi_claims_hosp --cov-report=term-missing)
```

The output will show the number of unit tests that passed and failed, along
with the percentage of code covered by the tests. 

None of the linting or unit tests should fail, and the code lines that are not covered by unit tests should be small and
should not include critical sub-routines. 


## Code tour

- run.py: reads params.json to generated updated signal, run daily
- update_indicator.py: 
    ClaimsHospIndicatorUpdater: reads the data, makes transformations, writes output
- indicator.py:
    ClaimsHospIndicator: methods for transforming data, including padding and smoothing
- load_data.py: methods for loading claims data
- smooth.py: left smoother for signal
- weekday.py: poisson model for handling day-of-week effects
- config.py: configuration variables