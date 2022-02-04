# COVID-19 Community Profile Report

The Data Strategy and Execution Workgroup (DSEW) publishes a Community Profile
Report each weekday at this location:

https://healthdata.gov/Health/COVID-19-Community-Profile-Report/gqxm-d9w9

This indicator extracts COVID-19 test figures from these reports.

Indicator-specific parameters:

* `input_cache`: a directory where Excel (.xlsx) files downloaded from
  healthdata.gov will be stored for posterity. Each file is 3.3 MB in size, so
  we expect this directory to require ~1GB of disk space for each year of
  operation.
* `reports`: {new | all | YYYY-mm-dd--YYYY-mm-dd} a string indicating which
  reports to export. The default, "new", downloads and exports only reports not
  already found in the input cache. The "all" setting exports data for all
  available reports, downloading them to the input cache if necessary. The date
  range setting refers to the date listed in the filename for the report,
  presumably the publish date. Only reports named with a date within the
  specified range (inclusive) will be downloaded to the input cache if necessary
  and exported.
* `export_start_date`: a YYYY-mm-dd string indicating the first date to export.
* `export_end_date`: a YYYY-mm-dd string indicating the final date to export.
* `export_signals`: list of string keys from constants.SIGNALS indicating which
  signals to export

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
env/bin/python -m delphi_dsew_community_profile
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
(cd tests && ../env/bin/pytest <your_test>.py --cov=delphi_dsew_community_profile --cov-report=term-missing)
```

The output will show the number of unit tests that passed and failed, along
with the percentage of code covered by the tests. 

None of the linting or unit tests should fail, and the code lines that are not covered by unit tests should be small and
should not include critical sub-routines. 
