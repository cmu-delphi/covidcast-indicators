# PACKAGE NAME

## Running the Indicator

The indicator is run by installing the package `delphiName` and running the script
"run.R". To install the pacakge, run the following code from this directory:

```
R CMD build delphiName
R CMD INSTALL delphiName_1.0.tar.gz
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
R -e 'roxygen2::roxygenise("delphiName")'
```

Testing the package is done with the build-in R package checks (which include both
static and dynamic checks), as well as unit tests written with **testthat**. To run all
of these, use the following from within this directory:

```
R CMD build delphiName
R CMD CHECK delphiName_1.0.tar.gz
```

None of the tests should fail and notes and warnings should be manually checked for issues.
To see the code coverage from the tests and example run the following:

```
Rscript -e 'covr::package_coverage("delphiName")'
```

There should be good coverage of all the core functions in the package.
