# Facebook Survey Response

## Running the Indicator

The Facebook symptoms surveys are one of our most complex pipelines, requiring a two-day process to accumulate all necessary input files in a production setting. This process is implemented in the `delphi_facebook` Python package and GNU make. For testing, we have included sample input files, so all that is needed is the `delphiFacebook` R package to run the actual indicator computations.

The indicator is run by installing the package `delphiFacebook` and running the script
"run.R". To install the package, run the following code from this directory:

```
make install-R
```

All of the user-changable parameters are stored in `params.json`. A template appropriate for running unit tests is
included as `params.json.template`.

To execute the module and produce the output datasets (by default, in
`receiving`), run the following:

```
Rscript run.R
```

## Building and testing the code

The documentation for the package is written using the **roxygen2** package. To
(re)-create this documentation for the package, run the following from the package
directory:

```
make lib
```

Testing the package is done with the built-in R package checks (which include both
static and dynamic checks), as well as unit tests written with **testthat**. To run all
of these, use the following from within this directory:

```
make test
```

None of the tests should fail and notes and warnings should be manually checked for issues.
To see the code coverage from the tests and example run the following:

```
make coverage
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
   by the `delphi_facebook` Python package using the Qualtrics API, managed by
   the `$(TODAY)` target of the Makefile. Successfully building this target
   requires a Qualtrics API token with access rights to the surveys through
   Qualtrics.
2. Read the survey data. This package extracts a unique token from each survey
   response and saves these to an output file; the automation script driving
   this package uses SFTP to send this file to Facebook.
3. Download the latest survey weights computed by Facebook for the tokens we
   provide. Facebook usually provides survey weights within one day, so our
   pipeline can produce weighted estimates a day after unweighted estimates. The
   download is managed by the `weights` target of the Makefile.
4. Aggregate the data and produce survey estimates.
5. Write the survey estimates to covidalert CSV files: one per day per signal
   type and geographic region type. (For example, there will be one CSV
   containing every unweighted unsmoothed county estimate for one signal on
   2020-05-10.)
6. Validate these estimates against basic sanity checks. (Not yet implemented in
   this pipeline!)
7. Push the CSV files to the API server. Implementation of this is left to the
   deployment environment (cron, DelphiAutomation, etc); for a sample script
   consult Katie.

Running the entire pipeline is managed through the `pipeline` target of the
Makefile, and assumes an extended `params.json` file modeled after
`params.json.production.template`. The template params file will not run the
complete pipeline on its own without first filling in the API token and the email
addresses of Facebook personnel who should be notified of irregularities in the
weights download procedure. Katie has this list; if you need it, ask her.

Mathematical details of how the survey estimates are calculated are given in the
signal descriptions [in the API
documentation](https://cmu-delphi.github.io/delphi-epidata/api/covidcast-signals/fb-survey.html).
This pipeline currently calculates two basic types of survey estimates:

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


The estimation code is driven by `aggregate.R`, which uses a tibble specifying
the details of every indicator to report. It handles aggregation to each geo
level and to each day (or 7-day average), and calculates each indicator at each
aggregation. Optionally, the days over which this is calculated are calculated
in parallel using `parallel::mclapply`.

Note that the details of aggregation are *highly* performance-critical. We
currently use `data.table` to do the filtering by geo and day, since its keys
and indices can do this filtering in O(log n) time. This is critical when
aggregating multiple weeks or months of data. Updates to the logic should be
careful to perform these filters the minimum number of times and to ensure they
still use the keys and indices.

## Output Files

The indicator produces three output types:

1. Aggregate estimates to covidalert CSV files, to publish in the API
2. "Individual" data, meaning complete survey responses in cleaned form, for
   sharing with research partners
3. CID lists, containing the unique identifiers for survey respondents (and
   nothing else), to send to Facebook so they can provide weights back to us.

Those outputs need to cover different time periods.

| Output | Function | Time period |
| ------ | -------- | ----------- |
| Aggregates | `aggregate_indicators` | [`start_date` - backfill period, `end_date`] |
| Individual data | `write_individual` | [`start_date` - backfill period, `end_date`] |
| CID lists | `write_cid` | [`start_date`, `end_date`] |

The backfill period is expected to be four days at most, since survey responses
on a specific date may be recorded a day or two later for various reasons.

Note that to calculate the aggregates on `start_date` - 4 days, we need to use
data from `start_date` - 4 days - 7 days, since we report 7-day smoothed
aggregates.

In typical use, we would expect `start_date` to be 00:00:00 on a given day, and
`end_date` to be 23:59:59 on that same day.

## Archiving

One particular challenge is created by our rule that we only consider the
*first* survey submission for a given CID, dated by the time the survey response
was *started*. If a Facebook user is invited to the survey, the link they
receive contains a unique CID intended for them; if they share the link with
friends or post it publicly, many other people may click the link and hence have
survey responses recorded with the same CID. Since the CIDs are used to generate
survey weights based on information about the user Facebook provided the link
to, weights for these additional responses would be wrong.

We assume the first opened survey is most likely to be the intended user, and
all following responses with the same CID are from others whose responses should
be excluded.

However, a response is only recorded when the user either completes the survey
or leaves it for more than four hours. This means the following could happen:

1. A user receives the survey link and opens the survey.
2. They pass the link to friends before completing the survey.
3. Numerous friends complete the survey quickly and their responses are
   recorded.
4. Meanwhile, the original user completes one question per hour.
5. Our pipeline runs and receives the responses from the friends -- but *not*
   from the original user, who is not done yet.
6. Finally, a day or two later, the original user finishes the survey and
   submits their response. Their response is dated as of the date they started
   the survey, meaning we must backfill our prior reported estimates.

We work around this problem by keeping an archive of all responses seen in the
past several weeks, stored as an RDS file. Each day, we do the following:

1. Load the data from the archive.
2. Load the newly received response data from the Qualtrics CSVs.
3. Merge the two datasets and sort them by survey start time.
4. Remove later responses with duplicated CIDs.
5. Proceed to aggregation and output processing.

Because a survey link can circulate online indefinitely, we must keep an archive
of *all* previously seen tokens and their date, though we do not need to support
backfill of responses submitted many weeks ago.

The archive, by retaining all responses, also is responsible for ensuring that
aggregate estimates that are multi-day averages function correctly even when the
pipeline is provided only a single day of Qualtrics CSV data.

We expect about four days of backfill, plus multi-day averages of a week, so the
archive needs to contain enough data to account for both. The `archive_days`
parameter in `params.json` controls this behavior.
