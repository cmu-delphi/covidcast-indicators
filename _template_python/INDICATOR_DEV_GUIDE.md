# Pipeline Development Manual


## A step-by-step guide to writing a pipeline

TODO:

* Geomapper guide
* Setting up development environment
* Deployment guide
* Manual for R?


## Introduction

This document provides a comprehensive guide on how to write a data pipeline in Python for the Delphi group.
It focuses on various aspects of building a pipeline, including ingestion, transformation, and storage.
This document assumes basic knowledge of Python and a familiarity with Delphi’s data processing practices.
Throughout the manual, we will use various python libraries to demonstrate how to build a data pipeline that can handle large volumes of data efficiently.
We will also discuss best practices for building reliable, scalable, and maintainable data pipelines.

### Related documents:

[Adding new API endpoints](https://cmu-delphi.github.io/delphi-epidata/new_endpoint_tutorial.html) (of which COVIDcast is a single example).

Most new data sources will be added as indicators within the main endpoint (called COVIDcast as of 2024-06-28).
In rare cases, it may be preferable to add a dedicated endpoint for a new indicator.
This would mainly be done if the format of the new data weren't compatible with the format used by the main endpoint, for example, if an indicator reports the same signal for many demographic groups, or if the reported geographic levels are nonstandard in some way.

[Setting up an S3 ArchiveDiffer](https://docs.google.com/document/d/1VcnvfeiO-GUUf88RosmNUfiPMoby-SnwH9s12esi4sI/edit#heading=h.e4ul15t3xmfj). Archive differs are used to compress data that has a long history that doesn't change that much. For example, the JHU CSSE indicator occasionally had revisions that could go back far in time, which meant that we needed to output all reference dates every day. Revisions didn't impact every location or reference date at a time, which meant that every issue would contain many values that were exactly the same as values issued the previous day. The archive differ removes those duplicates.

[Indicator debugging guide](https://docs.google.com/document/d/1vaNgQ2cDrMvAg0FbSurbCemF9WqZVrirPpWEK0RdATQ/edit): somewhat out-of-date but might still be useful


## Basic steps of an indicator

This is the general extract-transform-load procedure used by all COVIDcast indicators:

1. Download data from the source.
   * This could be via an [API query](https://github.com/cmu-delphi/covidcast-indicators/blob/fe39ebb1f8baa76670eb665d1dc99376ddfd3010/nssp/delphi_nssp/pull.py#L30), scraping a website, [an SFTP](https://github.com/cmu-delphi/covidcast-indicators/blob/fe39ebb1f8baa76670eb665d1dc99376ddfd3010/changehc/delphi_changehc/download_ftp_files.py#L19) or S3 dropbox, an email attachment, etc.
2. Process the source data to extract one or more time-series signals.
   * A signal includes a value, standard deviation (data-dependent), and sample size (data-dependent) for each region for each unit of time (a day or an epidemiological week "epi-week").
3. Aggregate each signal to all possible standard higher geographic levels.
   * For example, we generate data at the state level by combining data at the county level.
4. Output each signal into a set of CSV files with a fixed format.
5. Run a set of checks on the output.
   * This ensures output will be accepted by the acquisition code and hunts for common signs of buggy code or bad source data.
6. (Data-dependent) Compare today's output with a cached version of what's currently in the API.
   * This converts dense output to a diff and reduces the size of each update.
7. Deliver the CSV output files to the `receiving/` directory on the API server.

Adding a new indicator typically means implementing steps 1-3. Step 4 is included via the function ` create_export_csv`. Steps 5 (the validator), 6 (the archive differ) and 7 (acquisition) are all handled by runners in production.
## Step 0: Keep revision history (important!)

If the data provider doesn’t provide or it is unclear if they provide historical versions of the data, immediately set up a script (bash, Python, etc) to automatically (e.g. cron) download the data every day and save locally with versioning.

This step has a few goals:

1. Determine if the data is revised over time
2. Understand the revision behavior in detail
3. If the data is revised, we want to save all possible versions, even before our pipeline is fully up

The data should be saved in _raw_ form – do not do any processing.
Our own processing (cleaning, aggregation, normalization, etc) of the data may change as the pipeline code develops and doing any processing up front could make the historical data incompatible with the final procedure.

Check back in a couple weeks to compare data versions for revisions.


## Step 1: Exploratory Analysis

The goal for exploratory analysis is to decide how the dataset does and does not fit our needs.
This information will be used in the [indicator documentation](https://cmu-delphi.github.io/delphi-epidata/api/covidcast_signals.html) and will warn us about potential difficulties in the pipeline, so this should be done thoroughly! Your goal is to become an expert on the ins and outs of the data source.

While some of this process might have been done already (i.e.
it was already decided that the data is useful),  it is still important to understand the properties of the dataset.
The main objective during this stage is to understand what the dataset looks like in its raw format, establish what transformations need to be done, and create a basic roadmap to accomplish all later setup tasks.

**What you want to establish:**

* Data fields that correspond to signals we want to report
* Reporting lag and schedule 
* Backfill behavior 
* Sample size
* Georesolution
* Limitations

Jupyter notebooks work particularly well for exploratory analysis but feel free to use whatever IDE/methodology works best for you.
Some of this analysis may be useful during statistical review later, so save your code!

If anything unusual comes up, discuss with the stakeholder (usually the original requestor of the data source, can also be [@RoniRos](https://www.github.com/RoniRos)).
The goal is to figure out how to handle any issues before getting into the details of implementation.

### Fetching the data

Download the data in whatever format suits you.
A one-off manual download is fine.
Don’t worry too much about productionizing the data-fetching step at this point.
(Although any code you write can be used later.)

Also check to see whether the data is coming from an existing source, e.g. NSSP and NCHS are accessed the same way, so when adding NSSP, we could reuse the API key and only needed to lightly modify the API calls for the new dataset.

Reading from a local file:

```{python}
import pandas as pd
df = pd.read_csv('/Users/lukeneureiter/Downloads/luke_cpr_test.csv')
```
Fetching from Socrata:

```{python}
import os
from sodapy import Socrata
token = os.environ.get("SODAPY_APPTOKEN")
client = Socrata("data.cdc.gov", token)
results = client.get("rdmq-nq56", limit=10**10)
df = pd.DataFrame.from_records(results, coerce_float=True)
```

### Detailed questions to answer

At this stage we want to answer the questions below (and any others that seem relevant) and consider how we might use the data before we determine that the source should become a pipeline.

* What raw signals are available in the data?
   * If the raw signals aren’t useful themselves, what useful signals could we create from these?
   * Discuss with the data requestor or consult the data request GitHub issue which signals they are interested in.
     If there are multiple potential signals, are there any known pros/cons of each one?
   * For each signal, we want to report a value, standard error (data-dependent), and sample size (data-dependent) for each region for each unit of time.
     Sample size is sometimes available as a separate “counts” signal.
* Are the signals available across different geographies? Can values be [meaningfully compared](https://cmu-delphi.github.io/delphi-epidata/api/covidcast-signals/google-symptoms.html#limitations) between locations?
   * Ideally, we want to report data at [county, MSA,  HRR, state, HHS, and nation levels](https://cmu-delphi.github.io/delphi-epidata/api/covidcast_geography.html) (US) or subregion level 2 (county, parish, etc), subregion level 1 (state, province, territory), and nation levels for other countries.
     Some data sources report these levels themselves.
     For those that don’t, we use the [`geomapper`](https://github.com/cmu-delphi/covidcast-indicators/blob/84d059751b646c0075f1a384741f2c1d80981269/_delphi_utils_python/delphi_utils/geomap.py) to aggregate up from smaller to larger geo types.
     For that tool to work, signals must be aggregatable (i.e.
   values have to be comparable between geos) and the data must be reported at supported geo types or at geo types that are mappable to supported geo types.
* What geographies might be included that are not standard? 
   * For example, some data sources report NYC as separate from New York State.
   * Others require special handling: D.C. and territories (Puerto Rico, Guam, U.S. Virgin Islands).
   * ! Sampling site, facility, or other data-specific or proprietary geographic division
      * The data may not be appropriate for inclusion in the main endpoint (called COVIDcast as of 20240628).
        Talk to [@dshemetov](https://www.github.com/dshemetov) (geomapper), [@melange396](https://www.github.com/melange396) (epidata, DB), and [@RoniRos](https://www.github.com/RoniRos) (PI) for discussion.
      * Should the data have its own endpoint?
      * Consider creating a PRD ([here](https://drive.google.com/drive/u/1/folders/155cGrc9Y7NWwygslCcU8gjL2AQbu5rFF) or [here](https://drive.google.com/drive/u/1/folders/13wUoIl-FjjCkbn2O8qH1iXOCBo2eF2-d)) to present design options.
* What is the sample size? Is this a privacy concern for us or for the data provider?
* How often is data missing?
   * For privacy, some sources only report when sample size is above a certain threshold
   * Missingness due to reporting pattern (e.g. no weekend reports)?
   * Will we want to and is it feasible to [interpolate missing values](https://github.com/cmu-delphi/covidcast-indicators/issues/1539)?
* Are there any aberrant values that don’t make sense? e.g. negative counts, out of range percentages, “manual” missingness codes (9999, -9999, etc)
* Does the data source revise their data? How often? By how much? Is the revision meaningful, or an artifact of data processing methods?
   * See raw data saved in [Step 0](#step-0-keep-revision-history-important)
* What is the reporting schedule of the data?
* What order of magnitude is the signal? (If it’s too small or too large, [this issue on how rounding is done](https://github.com/cmu-delphi/covidcast-indicators/issues/1945) needs to be addressed first)
* How is the data processed by the data source? E.g. normalization, censoring values with small sample sizes, censoring values associated with low-population areas, smoothing, adding jitter, etc.
  Keep any code and notes around! They will be helpful for later steps.
  For any issues that come up, consider now if
  * We’ve seen them before in another dataset and, if so, how we handled it.
      Is there code around that we can reuse?
  * If it’s a small issue, how would you address it? Do you need an extra function to handle it?
  * If it’s a big issue, talk to others and consider making a PRD to present potential solutions.


## Step 2: Pipeline Code

Now that we know the substance and dimensions of our data, we can start planning the pipeline code.

### Logic overview

Broadly speaking, the objective here is to create a script that will download data, transform it (mainly by aggregating it to different geo levels), format it to match our standard format, and save the transformed data to the [receiving directory](https://github.com/cmu-delphi/covidcast-indicators/blob/d36352b/ansible/templates/changehc-params-prod.json.j2#L3) as a CSV.
The indicator, [validation](https://github.com/cmu-delphi/covidcast-indicators/tree/6912077acba97e835aff7d0cd3d64309a1a9241d/_delphi_utils_python/delphi_utils/validator) (a series of quality checks), and [archive diffing](https://github.com/cmu-delphi/covidcast-indicators/blob/6912077acba97e835aff7d0cd3d64309a1a9241d/_delphi_utils_python/delphi_utils/archive.py) (compressing the data by only outputting rows changed between data versions) are run via the runner.
Acquisition (ingestion of files from the receiving directory and into the database) is run separately (see the [`delphi-epidata repo`](https://github.com/cmu-delphi/delphi-epidata/tree/c65d8093d9e8fed97b3347e195cc9c40c1a5fcfa)).

`params.json.template` is copied to `params.json` during a run.
`params.json` is used to set parameters that modify a run and that we expect we’ll want to change in the future e.g. date range to generate) or need to be obfuscated (e.g. API key).

Each indicator includes a makefile (using GNU make), which provides predefined routines for local setup, testing, linting, and running the indicator.
At the moment, the makefiles use python 3.8.15+.

### Development

To get started, Delphi has a [basic code template](https://github.com/cmu-delphi/covidcast-indicators/tree/6f46f2b4a0cf86137fda5bd58025997647c87b46/_template_python) that you should copy into a top-level directory in the [`covidcast-indicators` repo](https://github.com/cmu-delphi/covidcast-indicators/).
It can also be helpful to read through other indicators, especially if they share a data source or format.

Indicators should be written in python for speed and maintainability.
Don't use R.

Generally, indicators have:

* `run.py`: Run through all the pipeline steps.
  Loops over all geo type-signal combinations we want to produce.
  Handles logging and saving to CSV using functions from [`delphi_utils`](https://github.com/cmu-delphi/covidcast-indicators/tree/6912077acba97e835aff7d0cd3d64309a1a9241d/_delphi_utils_python/delphi_utils).
* `pull.py`: Fetch the data from the data source and do basic processing (e.g. drop unnecessary columns).
  Advanced processing (e.g. sensorization) should go elsewhere.
* `geo.py`: Do geo-aggregation.
  This tends to be simple wrappers around [`delphi_utils.geomapper`](https://github.com/cmu-delphi/covidcast-indicators/blob/6912077acba97e835aff7d0cd3d64309a1a9241d/_delphi_utils_python/delphi_utils/geomap.py) functions.
  Do other geo handling (e.g. finding and reporting DC as a state).
* `constants.py`: Lists of geos to produce, signals to produce, dataset ids, data source URL, etc.

Your code should be _extensively_ commented! Especially note sections where you took an unusual approach (make sure to say why and consider briefly discussing alternate approaches).

#### Function stubs

If you have many functions you want to implement and/or anticipate a complex pipeline, consider starting with [function stubs](https://en.wikipedia.org/wiki/Method_stub) with comments or pseudo code.
Bonus: consider writing unit tests upfront based on the expected behavior of each function.

Some stubs to consider: 

* Retrieve a list of filenames
* Download one data file (API call, csv reader, etc.)
* Iterate through filenames to download all data files
* Construct an SQL query
* Run an SQL query
* Keep a list of columns
* Geographic transformations (tend to be wrappers around [`delphi_utils.geomapper`](https://github.com/cmu-delphi/covidcast-indicators/blob/6912077acba97e835aff7d0cd3d64309a1a9241d/_delphi_utils_python/delphi_utils/geomap.py) functions)

Example stub:

```{python}
def api_call(args)
    #implement api call
    return df
```

Next, populate the function stubs with the intention of using them for a single pre-defined run (ignoring params.json, other geo levels, etc).
If you fetched data programmatically in Step 0, you can reuse that in your data-fetching code.
If you reformatted data in Step 1, you can reuse that too.
Below is an example of the function stub that has been populated with code for a one-off run.

```{python}
def api_call(token: str):
    client = Socrata('healthdata.gov', token)
    results = client.get("di4u-7yu6", limit=5000)
    results_df = pd.DataFrame.from_records(results)
    return results_df
```

After that, generalize your code to be able to be run on all geos of interest, take settings from params.json, use constants for easy maintenance, with extensive documentation, etc.

#### Development environment

Make sure you have a functional environment with python 3.8.15+.
For local runs, the makefile’s make install target will set up a local virtual environment with necessary packages.

(If working in R (very much NOT recommended), local runs can be run without a virtual environment or using the [`renv` package](https://rstudio.github.io/renv/articles/renv.html), but production runs should be set up to use Docker.)

#### Dealing with data-types

* Often problem encountered prior to geomapper 
   * Problems that can arise and how to address them 
* Basic conversion

TODO: A list of assumptions that the server makes about various columns would be helpful.
E.g. which geo values are allowed, should every valid date be present in some way, etc

#### Dealing with geos

In an ideal case, the data exists at one of our [already covered geos](https://cmu-delphi.github.io/delphi-epidata/api/covidcast_geography.html):

* State: state_code (string, leftpadded to 2 digits with 0) or state_id (string)
* FIPS (state+county codes, string leftpadded to 5 digits with 0)
* ZIP
* MSA (metro statistical area, int)
* HRR (hospital referral region, int)

If you want to map from one of these to another, the [`delphi_utils.geomapper`](https://github.com/cmu-delphi/covidcast-indicators/blob/6912077acba97e835aff7d0cd3d64309a1a9241d/_delphi_utils_python/delphi_utils/geomap.py) utility covers most cases.
A brief example of aggregating from states to hhs regions via their population:

```{python}
from delphi_utils.geomap import GeoMapper
geo_mapper = GeoMapper()
geo_mapper.add_geocode(df, "state_id", "state_code", from_col = "state") # add codes and ids from the full names
df = geo_mapper.add_population_column(df, "state_code") # add state populations
hhs_version = geo_mapper.replace_geocode(df, "state_code","hhs", new_col = "geo_id") # aggregate to hhs regions, renaming the geo column to geo_id
```

This example is taken from [`hhs_hosp`](https://github.com/cmu-delphi/covidcast-indicators/blob/main/hhs_hosp/delphi_hhs/run.py); more documentation can be found in the `geomapper` class definition.

#### Implement a Missing Value code system

The column is described [here](https://cmu-delphi.github.io/delphi-epidata/api/missing_codes.html).

#### Local testing

As a general rule, it helps to decompose your functions into operations for which you can write unit tests.
To run the tests, use `make test` in the top-level indicator directory.

Unit tests are required for all functions.
Integration tests are highly desired, but may be difficult to set up depending on where the data is being fetched from.
Mocking functions are useful in this case.

#### Naming

Indicator and signal names need to be approved by [@RoniRos](https://www.github.com/RoniRos).
It is better to start that conversation sooner rather than later.

The data source name as specified during an API call (e.g. in `epidatr::pub_covidcast(source = "jhu-csse", ...)`, "jhu-csse" is the data source name) should match the wildcard portion of the module name ("jhu" in `delphi_jhu`) _and_ the top-level directory name in `covidcast-indicators` ("jhu").
(Ideally, these would all also match how we casually refer to the indicator ("JHU"), but that's hard to foresee and enforce.)

Ideally, the indicator name should:

* Make it easy to tell where the data is coming from
* Make it easy to tell what type of data it is and/or what is unique about it
* Be uniquely identifying enough that if we added another indicator from the same organization, we could tell the two apart
* Be fairly short
* Be descriptive

Based on these guidelines, the `jhu-csse` indicator would be better as `jhu-csse` everywhere (module name could be `delphi_jhu_csse`), rather than having a mix of `jhu-csse` and `jhu`.

Signal names should not be too long, but the most important feature is that they are descriptive.
If we're mirroring a processed dataset, consider keeping their signal names.

Use the following standard tags when creating new signal names:

* `raw`: unsmoothed, _no longer used; if no smoothing is specified the signal is assumed to be "raw"_
* `7dav`: smoothed using a average over a rolling 7-day window; comes at the end of the name
* `smoothed`: smoothed using a more complex smoothing algorithm; comes at the end of the name
* `prop`: counts per 100k population
* `pct`: percentage between 0 and 100
* `num`: counts, _no longer used; if no value type is specified the signal is assumed to be a count_
* `cli`: COVID-like illness (fever, along with cough or shortness of breath or difficulty breathing)
* `ili`: influenza-like illness (fever, along with cough or sore throat)

Using this tag dictionary, we can interpret the following signals as

* `confirmed_admissions_influenza_1d_prop` = raw (unsmoothed) daily ("1d") confirmed influenza hospital admissions ("confirmed_admissions_influenza") per 100,000 population ("prop").
* `confirmed_admissions_influenza_1d_prop_7dav` = the same as above, but smoothed with a 7-day moving average ("7dav").

### Statistical review

The data produced by the new indicator needs to be sanity-checked.
Think of this as doing [exploratory data analysis](#step-1-exploratory-analysis) again, but on the pipeline _output_.
Some of this does overlap with work done in Step 1, but should be revisited following our processing of the data.
Aspects of this investigation will be useful to include in the signal documentation.

The analysis doesn't need to be formatted as a report, but should be all in one place, viewable by all Delphi members, and in a format that makes it easy to comment on.
Some good options are the GitHub issue originally requesting the data source and the GitHub pull request adding the indicator.

There is not a formal process for this, and you're free to do whatever you think is reasonable and sufficient.
A thorough analysis would cover the following topics:

* Run the [correlations notebook](https://github.com/cmu-delphi/covidcast/blob/5f15f71/R-notebooks/cor_dashboard.Rmd) ([example output](https://cmu-delphi.github.io/covidcast/R-notebooks/signal_correlations.html#)).
   * This helps evaluate the potential value of the signals for modeling.
   * Choropleths give another way to plot the data to look for weird patterns.
   * Good starting point for further analyses.
* Compare the new signals against pre-existing relevant signals
   * For signals that are ostensibly measuring the same thing, this helps us see issues and benefits of one versus the other and how well they agree (e.g. [JHU cases vs USAFacts cases](https://github.com/cmu-delphi/covidcast-indicators/issues/991)).
   * For signals that we expect to be related, we should see correlations of the right sign and magnitude.
* Plot all signals over time.
   * (unlikely) Do we need to do any interpolation?
   * (unlikely) Think about if we should do any filtering/cleaning, e.g. [low sample size](https://github.com/cmu-delphi/covidcast-indicators/issues/1513#issuecomment-1036326474) in covid tests causing high variability in test positivity rate.
* Plot all signals for all geos over time and space (via choropleth).
   * Look for anomalies, missing geos, missing-not-at-random values, etc.
   * Verify that DC and any territories are being handled as expected.
* Think about [limitations](https://cmu-delphi.github.io/delphi-epidata/api/covidcast-signals/jhu-csse.html#limitations), gotchas, and lag and backfill characteristics.

[Example analysis 1](https://github.com/cmu-delphi/covidcast-indicators/pull/1495#issuecomment-1039477646), [example analysis 2](https://github.com/cmu-delphi/covidcast-indicators/issues/367#issuecomment-717415555).

Once the analysis is complete, have the stakeholder (usually the original requestor of the data source, can also be [@RoniRos](https://www.github.com/RoniRos)) review it.

### Documentation

The [documentation site](https://cmu-delphi.github.io/delphi-epidata/) ([code here](https://github.com/cmu-delphi/delphi-epidata/tree/628e9655144934f3903c133b6713df4d4fcc613e/docs)) stores long-term long-form documentation pages for each indicator, including those that are inactive.

Active and new indicators go in the [COVIDcast Main Endpoint -> Data Sources and Signals](https://cmu-delphi.github.io/delphi-epidata/api/covidcast_signals.html) section ([code here](https://github.com/cmu-delphi/delphi-epidata/tree/628e9655144934f3903c133b6713df4d4fcc613e/docs/api/covidcast-signals)).
A [template doc page](https://github.com/cmu-delphi/delphi-epidata/blob/628e9655144934f3903c133b6713df4d4fcc613e/docs/api/covidcast-signals/_source-template.md) is available in the same directory.

An indicator documentation page should contain as much detail (including technical detail) as possible.
The following fields are required:

* Description of the data source and data collection methods
* Links to the data source (organization and specific dataset(s) used)
* Links to any data source documentation you referenced
* List of signal names, descriptions, with start dates
* Prose description of how signals are calculated
* Specific math showing how signals are calculated, if unusual or complex or you like equations
* How smoothing is done, if any
* Known limitations of the data source and the final signals
* Missingness characteristics, especially if the data is missing with a pattern (on weekends, specific states, etc)
* Lag and revision characteristics
* Licensing information

and anything else that changes how users would use or interpret the data, impacts the usability of the signal, may be difficult to discover, recommended usecases, is unusual, any gotchas about the data or the data processing approach, etc.
_More detail is better!_

At the time that you're writing the documentation, you are the expert on the data source and the indicator.
Making the documentation thorough and clear will make the data maximally usable for future users, and will make maintenance for Delphi easier.

(For similar reasons, comment your code extensively!)

## Step 3: Deployment

* This is after we have a working one-off script 
* Using Delphi utils and functionality 
* What happens to the data after it gets put in `receiving/`:

Next, the `acquisition.covidcast` component of the `delphi-epidata` codebase does the following immediately after an indicator run (you need to set acquisition job up):

1. Look in the `receiving/` folder to see if any new data files are available.
   If there are, then:
   1. Import the new data into the epimetric_full table of the epidata.covid database, filling in the columns as follows:
      1. `source`: parsed from the name of the subdirectory of `receiving/`
      2. `signal`: parsed from the filename
      3. `time_type`: parsed from the filename
      4. `time_value`: parsed from the filename
      5. `geo_type`: parsed from the filename
      6. `geo_value`: parsed from each row of the csv file
      7. `value`: parsed from each row of the csv file
      8. `se`: parsed from each row of the csv file
      9. `sample_size`: parsed from each row of the csv file
      10. `issue`: whatever now is in time_type units
      11. `lag`: the difference in time_type units from now to time_value
      12. `value_updated_timestamp`: now
   2. Update the `epimetric_latest` table with any new keys or new versions of existing keys.

Consider what settings to use in the `params.json.template` file in accordance with how you want to run the indicator and acquisition.
Pay attention to the receiving directory, as well as how you can store credentials in vault.
Refer to [this guide](https://docs.google.com/document/d/1Bbuvtoxowt7x2_8USx_JY-yTo-Av3oAFlhyG-vXGG-c/edit#heading=h.8kkoy8sx3t7f) for more vault info.

### CI/CD

* Add module name to the `build` job in `.github/workflows/python-ci.yml`.
  This allows github actions to run on this indicator code, which includes unit tests and linting.
* Add top-level directory name to `indicator_list` in `Jenkinsfile`.
  This allows your code to be automatically deployed to staging after your branch is merged to main, and deployed to prod after `covidcast-indicators` is released.
* Create `ansible/templates/{top_level_directory_name}-params-prod.json.j2` based on your `params.json.template` with some adjustment:
   * "export_dir": "/common/covidcast/receiving/{data-source-name}"
   * "log_filename": "/var/log/indicators/{top_level_directory_name}.log"

Pay attention to the receiving/export directory, as well as how you can store credentials in vault.
Refer to [this guide](https://docs.google.com/document/d/1Bbuvtoxowt7x2_8USx_JY-yTo-Av3oAFlhyG-vXGG-c/edit#heading=h.8kkoy8sx3t7f) for more vault info.

### Staging

After developing the pipeline code, but before deploying in development, the pipeline should be tested on staging.
Indicator runs should be set up to run automatically daily for at least a week.

The indicator run code is automatically deployed on staging after your branch is merged into `main`.
After merging, make sure you have proper access to Cronicle and staging server `app-mono-dev-01.delphi.cmu.edu` _and_ can see your code on staging at `/home/indicators/runtime/`.

Then, on Cronicle, create two jobs: one to run the indicator and one to load the output csv files into database. 

We start by setting up the acquisition job.

#### Acquisition job

The indicator job loads the location of the relevant csv output files into chained data, which this acquisition job then loads into our database.

Example script:

```
#!/usr/bin/python3

import subprocess
import json

str_data = input()
print(str_data)

data = json.loads(str_data, strict=False)
chain_data = data["chain_data"]
user = chain_data["user"]
host = chain_data["host"]
acq_ind_name = chain_data["acq_ind_name"]

cmd = f'''ssh -T -l {user} {host} "cd ~/driver && python3 -m delphi.epidata.acquisition.covidcast.csv_to_database --data_dir=/common/covidcast --indicator_name={acq_ind_name} --log_file=/var/log/epidata/csv_upload_{acq_ind_name}.log"'''

std_err, std_out = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()

print(std_err.decode('UTF-8'))
print(std_out.decode('UTF-8'))
```

#### Indicator run job

This job signs into our staging server via ssh, runs the indicator, producing csv files as output.

Example script:

```
#!/bin/sh

# vars
user='automation'
host='app-mono-dev-01.delphi.cmu.edu'
ind_name='nchs_mortality'
acq_ind_name='nchs-mortality'

# chain_data to be sent to acquisition job
chain_data=$(jo chain_data=$(jo acq_ind_name=${acq_ind_name} ind_name=${ind_name} user=${user} host=${host}));
echo "${chain_data}";

ssh -T -l ${user} ${host} "sudo -u indicators -s bash -c 'cd /home/indicators/runtime/${ind_name} && env/bin/python -m delphi_${ind_name}'";
```

Note the staging hostname in `host` and how the acquisition job is chained to run right after the indicator job.

Note that `ind_name` variable here refer to the top-level directory name where code is located, while `acq_ind_name` refer to the directory name where output csv files are located, which corresponds to the name of `source` column in our database, as mentioned in step 3.

To automatically run acquisition job right after indicator job finishes successfully:

1. In `Plugin` section, select `Interpret JSON in Output`.
2. In `Chain Reaction` section, select your acquisition run job below to `Run Event on Success`

You can read more about how the `chain_data` json object in the script above can be used in our subsequent acquisition job [here](https://github.com/jhuckaby/Cronicle/blob/master/docs/Plugins.md#chain-reaction-control).

#### Staging database checks

Apart from checking the logs of staging indicator run and acquisition jobs to identify potential issues with the pipeline, one can also check the contents of staging database for abnormalities.

At this point, acquisition job should have loaded data onto staging mysql db, specifically the `covid` database.

From staging:
```
[user@app-mono-dev-01 ~]$ mysql -u user -p
Enter password: 
Welcome to the MySQL monitor.  Commands end with ; or \g.
Your MySQL connection id is 00000
Server version: 8.0.36-28 Percona Server (GPL), Release 28, Revision 47601f19

Copyright (c) 2009-2024 Percona LLC and/or its affiliates
Copyright (c) 2000, 2024, Oracle and/or its affiliates.

Oracle is a registered trademark of Oracle Corporation and/or its
affiliates. Other names may be trademarks of their respective
owners.

Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

mysql> use covid;
Database changed
```
Check `signal_dim` table to see if new source and signal names are all present and reasonable. For example:
```
mysql> select * from signal_dim where source='nssp';
+---------------+--------+----------------------------------+
| signal_key_id | source | signal                           |
+---------------+--------+----------------------------------+
|           817 | nssp   | pct_ed_visits_combined           |
|           818 | nssp   | pct_ed_visits_covid              |
|           819 | nssp   | pct_ed_visits_influenza          |
|           820 | nssp   | pct_ed_visits_rsv                |
|           821 | nssp   | smoothed_pct_ed_visits_combined  |
|           822 | nssp   | smoothed_pct_ed_visits_covid     |
|           823 | nssp   | smoothed_pct_ed_visits_influenza |
|           824 | nssp   | smoothed_pct_ed_visits_rsv       |
+---------------+--------+----------------------------------+
```

Then, check if the number of records ingested in db matches with the number of rows in csv when running locally.
For example, the below query sets the `issue` date being the day acquisition job was run, and `signal_key_id` correspond with signals from our new source.
Check if this count matches with local run result.

```
mysql> SELECT count(*) FROM epimetric_full WHERE issue=202425 AND signal_key_id > 816 AND signal_key_id < 825;
+----------+
| count(*) |
+----------+
|  2620872 |
+----------+
1 row in set (0.80 sec)
```

You can also check how data looks more specifically at each geo level or among different signal names depending on the quirks of the source.

See [@korlaxxalrok](https://www.github.com/korlaxxalrok) or [@minhkhul](https://www.github.com/minhkhul) for more information.

If everything goes well make a prod version of the indicator run job and use that to run indicator on a daily basis.

### Signal Documentation

TODO

Apparently adding to a google spreadsheet, need to talk to someone (Carlyn) about the specifics

How to add to signal discovery app

How to add to www-main signal dashboard

Github page signal documentation talk to [@nmdefries](https://www.github.com/nmdefries) and [@tinatownes](https://www.github.com/tinatownes)
