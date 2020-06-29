# Facebook Survey Response

## Running the Indicator

The indicator is run by installing the package `delphiFacebook` and running the script
"run.R". To install the pacakge, run the following code from this directory:

```
R CMD build delphiFacebook
R CMD INSTALL delphiFacebook_1.0.tar.gz
```

All of the user-changable parameters are stored in `params.json`. A template is
included as `params.json.template`.

To execute the module and produce the output datasets (by default, in
`receiving`), run the following:

```
Rscript run.R
```

## Building and testing the code

The documentation for the package is written using the **roxygen2** packaage. To
(re)-create this documentation for the package, run the following from the package
directory:

```
R -e 'roxygen2::roxygenise("delphiFacebook")'
```

Testing the package is done with the build-in R package checks (which include both
static and dynamic checks), as well as unit tests written with **testthat**. To run all
of these, use the following from within this directory:

```
R CMD build delphiFacebook
R CMD CHECK delphiFacebook_1.0.tar.gz
```

None of the tests should fail and notes and warnings should be manually checked for issues.
To see the code coverage from the tests and example run the following:

```
Rscript -e 'covr::package_coverage("delphiFacebook")'
```

There should be good coverage of all the core functions in the package.

### Writing tests

Because the package tests involve reading and writing many files, we must be
careful with working directories to ensure the tests are portable.

For reading and writing to files contained in the `tests/testthat/` directory,
use the `testthat::test_path` function. It works much like `file.path` but
automatically provides paths relative to `tests/testthat/`, so e.g.
`test_path("input")` becomes `tests/testthat/input/` or whatever relative path
is needed to get there.

`params.json` files contain paths, so `tests/testthat/helper-relativize.R`
contains `relativize_params`, which takes a `params` list and applies
`test_path` to all of its path components. This object can then be passed to
anything that needs it to read or write files.

### Testing during development

Repeatedly building the package and running the full check suite is tedious if
you are working on fixing a failing test. A faster workflow is this:

1. Set your R working directory to `delphiFacebook/tests/testthat`.
2. Run `testthat::test_dir('.')`

This will test the live code without having to rebuild the package.

## Outline of the Indicator

Facebook surveys are one of our most complex data pipelines. At a high level,
the pipeline must do the following:

1. Download the latest survey from Qualtrics and place it in a CSV. This is done
   by a Python script using the Qualtrics API (not yet in this repository).
2. Read the survey data. This package extracts a unique token from each survey
   response and saves these to an output file; the automation script driving
   this package uses SFTP to send this file to Facebook.
3. Download the latest survey weights computed by Facebook for the tokens we
   provide. Facebook usually provides survey weights within one day, so our
   pipeline can produce weighted estimates a day after unweighted estimates. The
   download is managed by the same automation script, not this package.
4. Aggregate the data and produce survey estimates.
5. Write the survey estimates to covidalert CSV files: one per day per signal
   type and geographic region type. (For example, there will be one CSV
   containing every unweighted unsmoothed county estimate for one signal on
   2020-05-10.)
6. Validate these estimates against basic sanity checks. (Not yet implemented in
   this pipeline!)
7. Push the CSV files to the API server. Also done by the automation script, not
   by this package.


Mathematical details of how the survey estimates are calculated are given in the
signal descriptions PDF in the `covid-19` repository. This pipeline currently
calculates two basic types of survey estimates:

1. Estimates of the fraction of individuals with COVID-like or influenza-like
   illness. The survey includes questions about how many people in your
   household have specific symptoms; we use the fractions within households to
   estimate a population rate of symptoms. This estimation is handled by
   `count.R` in the `delphiFacebook` package.
2. Estimates of the fraction of people who know someone in their local community
   with symptoms. Here we do not distinguish between COVID-like and
   influenza-like symptoms. We produce separate estimates including or excluding
   within-household symptoms, and do not use the number of people the respondent
   knows, only whether or not they know someone with symptoms. This estimation
   is handled by `binary.R` in the `delphiFacebook` package.
