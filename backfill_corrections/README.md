# Backfill Correction

## Running the Pipeline

The indicator is run by installing the package `delphiBackfillCorrection` and
running the script "run.R". To install the package, run the following code
from this directory:

```
make install
```

All of the user-changable parameters are stored in `params.json`. A basic
template is included as `params.json.template`. Default values are provided
for most parameters; `input_dir` is the only requied parameter.

To execute the module and produce the output datasets (by default, in
`receiving`), run the following:

```
Rscript run.R
```

The functions in `tooling.R` are provided as a user-friendly way to run
backfill corrections on any dataset that the user has on hand. This local
processing can be done by running the following from this directory:

```
Rscript correct_local_signal.R
```

Default values are provided for most parameters; `input_dir`,
`test_start_date`, and `test_end_date` must be provided as command line
arguments.

## Building and testing the code

The documentation for the package is written using the **roxygen2** package. To
(re)-create this documentation for the package, run the following from the package
directory:

```
make lib
```

Testing the package is done with the built-in R package checks (which include
both static and dynamic checks), as well as unit tests written with
**testthat**. To run all of these, use the following from within this
directory:

```
make test
```

None of the tests should fail and notes and warnings should be manually
checked for issues. To see the code coverage from the tests and example run
the following:

```
make coverage
```

There should be good coverage of all the core functions in the package.

### Writing tests

Because the package tests involve reading and writing files, we must be
careful with working directories to ensure the tests are portable.

For reading and writing to files contained in the `unit-tests/testthat/` directory,
use the `testthat::test_path` function. It works much like `file.path` but
automatically provides paths relative to `unit-tests/testthat/`, so e.g.
`test_path("input")` becomes `unit-tests/testthat/input/` or whatever relative path
is needed to get there.

`params.json` files contain paths, so `unit-tests/testthat/helper-relativize.R`
contains `relativize_params`, which takes a `params` list and applies
`test_path` to all of its path components. This object can then be passed to
anything that needs it to read or write files.

### Testing during development

Repeatedly building the package and running the full check suite is tedious if
you are working on fixing a failing test. A faster workflow is this:

1. Set your R working directory to `delphiBackfillCorrection/unit-tests/testthat`.
2. Run `testthat::test_dir('.')`

This will test the live code without having to rebuild the package.

## Outline of the Indicator

TODO

### Data requirements

Required columns with fixed column names:

- geo_value: strings or floating numbers to indicate the location
- time_value: reference date
- lag: the number of days between issue date and the reference date
- issue_date: issue date/report, required if lag is not available

Required columns without fixed column names (column names must be specified in [TODO]):

- num_col: the column for the number of reported counts of the numerator. e.g.
  the number of COVID claims counts according to insurance data.
- denom_col: the column for the number of reported counts of the denominator.
  e.g. the number of total claims counts according to insurance data. Required
  if correcting ratios.

## Output Files

The pipeline produces two output types:

1. Predictions

| geo_value | time_value |lag | value | predicted_tauX | ... | wis | 
|--- | --- | --- | --- |--- |--- |--- |
| pa | 2022-01-01 | 1 | 0.1 | 0 | ... | 0.01 |

3. Model objects. In production, models are trained on the last year of
   versions (as-of dates) and the last year of reference (report) dates. For
   one signal at the state level, a model takes about 30 minutes to train. Due
   to resource limitations in production, we only train models once a month
   and save the model objects between runs. By default, these are saved to the
   `cache` directory name with suffix `.model`.
   
