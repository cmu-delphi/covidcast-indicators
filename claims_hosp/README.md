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

## Running Patches

To regenerate data for a range of issue dates in batch issue format, turn on the
`custom_run` flag and add a `patch` section to `params.json` as described in
`patch.py`, then run

```
env/bin/python -m delphi_claims_hosp.patch
```

An issue is a full re-run of the indicator against the drop that arrived that
day, not one day of data: each issue re-emits `n_backfill_days` of `time_value`s,
exactly as the daily run would have.

Each issue pulls the drop that arrived on its issue date from the ftp server, so
patching needs working ftp credentials. How far back the server keeps drops is a
property of the server; for older issues, stage the drops in
`indicator.input_dir` yourself and the downloader will skip over them. Issue
dates with no drop available are logged and skipped.

A patch leaves `input_dir` populated when it finishes rather than clearing it the
way a daily run does, since later issues in the range still need the earlier
drops. Point patches at their own `input_dir` to keep them out of the staging
directory the daily run uses.

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
- patch.py: runs the indicator over a range of issue dates, for backfilling
    missed issues
- backfill.py: stores daily backfill files and merges them; also folds a patched
    issue back into the merged file that covers its date
- update_indicator.py: 
    ClaimsHospIndicatorUpdater: reads the data, makes transformations, writes output
- indicator.py:
    ClaimsHospIndicator: methods for transforming data, including padding and smoothing
- load_data.py: methods for loading claims data
- smooth.py: left smoother for signal
- weekday.py: poisson model for handling day-of-week effects
- config.py: configuration variables