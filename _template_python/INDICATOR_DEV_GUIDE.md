# Pipeline Development Manual

## A step-by-step guide to writing a pipeline

TODO:

[] Geomapper guide
[] Setting up development environment
[] Deployment guide
[] Manual for R?


## Introduction

This document provides a comprehensive guide on how to write a data pipeline in Python for the Delphi group. It focuses on various aspects of building a pipeline, including ingestion, transformation, and storage. This document assumes basic knowledge of Python and a familiarity with Delphi’s data processing practices. Throughout the manual, we will use various python libraries to demonstrate how to build a data pipeline that can handle large volumes of data efficiently. We will also discuss best practices for building reliable, scalable, and maintainable data pipelines. 

### Related documents:

There is a guide to new endpoints (of which COVIDcast is a single example) here in delphi-epidata, and hosted on the actual website here.

## Basic steps of an indicator

This is the general extract-transform-load procedure used by all COVIDcast indicators:

1. Download data from the source. 
   * This could be via an API query, scraping a website, an SFTP or S3 dropbox, an email attachment, etc.
2. Process the source data to extract one or more time-series signals. 
   * A signal includes a value, standard error (data-dependent), and sample size (data-dependent) for each region for each unit of time (a day or an epidemiological week "epi-week").
3. Aggregate each signal to all possible standard higher geographic levels. 
   * For example, we generate data at the state level by combining data at the county level.
4. Output each signal into a set of CSV files with a fixed format.
5. Run a set of checks on the output.
   * This ensures output will be accepted by the acquisition code and hunts for common signs of buggy code or bad source data.
6. (Data-dependent) Compare today's output with a cached version of what's currently in the API.
   * This converts dense output to a diff and reduces the size of each update.
7. Deliver the CSV output files to the receiving/ directory on the API server.

## Stage 0: Keep revision history (important!)

If the data provider doesn’t provide or it is unclear if they provide historical versions of the data, immediately set up a script (bash, Python, etc) to automatically (e.g. cron) download the data every day and save locally with versioning.

This step has a few goals:

1. Determine if the data is revised over time
2. Understand the revision behavior in detail
3. If the data is revised, we want to save all possible versions, even before our pipeline is fully up

The data should be saved in raw form – do not do any processing. Our own processing (cleaning, aggregation, normalization, etc) of the data may change as the pipeline code develops and doing any processing up front could make the historical data incompatible with the final procedure.
Check back in a couple weeks to compare data versions for revisions.

## Stage 1: Exploratory Analysis

The goal for exploratory analysis is to decide how the dataset does and does not fit our needs. This information will be used in the indicator documentation and will warn us about potential difficulties in the pipeline, so this should be done thoroughly! Your goal is to become an expert on the ins and outs of the data source.

While some of this process might have been done already (i.e. it was already decided that the data is useful),  it is still important to understand the properties of the dataset. The main objective during this stage is to understand what the dataset looks like in its raw format, establish what transformations need to be done, and create a basic roadmap to accomplish all later setup tasks. 

What you want to establish:

* Data fields that correspond to signals we want to report
* Reporting lag and schedule 
* Backfill behavior 
* Sample size
* Georesolution
* Limitations

### Fetching the data

Download the data in whatever format suits you. Jupyter notebooks work particularly well for exploratory analysis but feel free to use whatever IDE/methodology works best for you.

* As an example Luke manually downloaded a CSV from the DSEW-CPR county website and used that for initial exploration in a jupyter notebook. Don’t worry too much about productionizing the data-fetching step now.
* Check to see whether the data is coming from an existing source, e.g. the wastewater data and NCHS data are accessed the same way, so when adding wastewater data, we could reuse the  API key and only needed to lightly modify the API calls for the new dataset.

From a local file:

```{python}
import pandas as pd
df = pd.read_csv('/Users/lukeneureiter/Downloads/luke_cpr_test.csv')
```
From Socrata:

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
   * Discuss with the data requestor or data request GitHub issue which signals they are interested in and, if there are multiple potential signals, the pros/cons of using each one.
   * For each signal, we want to report a value, standard error (data-dependent), and sample size (data-dependent) for each region for each unit of time. Sample size is sometimes available as a separate “counts” signal.
* Are the signals available across different geographies? Can values be meaningfully compared between locations?
   * Ideally, we want to report data at county, MSA,  HRR, state, HHS, and nation levels (US) or subregion level 2 (county, parish, etc), subregion level 1 (state, province, territory), and nation levels for other countries. Some data sources report these levels themselves. For those that don’t, we use the geomapper to aggregate up from smaller to larger geo types. For that tool to work, signals must be aggregatable (i.e. values have to be comparable between geos) and the data must be reported at supported geo types or at geo types that are mappable to supported geo types.
* What geographies might be included that are not standard? 
   * For example, some data sources report NYC as separate from New York State.
   * Others require special handling: D.C., Puerto Rico, Guam, U.S. Virgin Islands.
   * ! Sampling site, facility, or other data-specific or proprietary geographic division
      * The data may not be appropriate for inclusion in the main endpoint (as of 20240628 called COVIDcast). Talk to Dmitry Shemetov(geomapper), George Haff(epidata, DB), and Roni Rosenfeld(PI) for discussion.
      * Should the data have its own endpoint?
      * Consider creating a PRD (here or here) to present design options.
* What is the sample size? Is this a privacy concern for us or for the data provider?
* How often is data missing?
   * E.g. for privacy, some sources only report when sample size is above a certain threshold
   * Will we want to and is it feasible to interpolate missing values?
* Are there any aberrant values that don’t make sense? e.g. negative counts, out of range percentages, “manual” missingness codes (9999, -9999, etc)
* Does the data source revise their data? How often?
   * See raw data saved in Stage 0
* What is the reporting schedule of the data?
* What order of magnitude is the signal? (If it’s sufficiently small, this issue needs to be addressed first)
* How is the data processed by the data source? E.g. normalization, censoring values with small sample sizes, censoring values associated with low-population areas, smoothing, adding jitter, etc.
Keep any code and notes around! They will be helpful for later steps.
For any issues that come up, consider now if
* We’ve seen them before in another dataset and, if so, how we handled it. Is there code around that we can reuse?
* If it’s a small issue, how would you address it? Do you need an extra function to handle it?
* If it’s a big issue, talk to others and consider making a PRD to present potential solutions.

## Stage 2: Pipeline Code

Now that we know the substance and dimensions of our data, we can start planning the pipeline code.

### Logic overview

Broadly speaking, the objective here is to create a script that will download data, transform it (mainly by aggregating it to different geo levels), format it to match our standard format, and save the transformed data to the receiving directory as a CSV. The indicator, validation (a series of quality checks), and archive diffing (compressing the data by only outputting rows changed between data versions) are run via the runner. Acquisition (ingestion of files from the receiving directory and into the database) is run separately (see the delphi-epidata repo).

params.json.template is copied to params.json during a run. params.json is used to set parameters that modify a run and that we expect we’ll want to change in the future (e.g. date range to generate) or need to be obfuscated (e.g. API key).

Each indicator has a makefile (using GNU make), which provides predefined routines for local setup, testing, linting, and running the indicator. At the moment, the makefiles use python 3.8.15+.

### Development

To get started, Delphi has a basic code template that you should copy into a top-level directory in the covidcast-indicators repo. It can also be helpful to read through other indicators (e.g.), especially if they share a data source or format.

Indicators should be written in python for speed and maintainability. If you think you need to use R, please reconsider! and talk to other engineering team members.

Generally, indicators have:

* run.py: Run through all the pipeline steps. Loops over all geo type-signal combinations we want to produce. Handles logging and saving to CSV using functions from delphi_utils.
* pull.py: Fetch the data from the data source and do basic processing (e.g. drop unnecessary columns). Advanced processing (e.g. sensorization) should go elsewhere.
* geo.py: Do geo-aggregation. This tends to be simple wrappers around delphi_utils.geomapper functions. Do other geo handling (e.g. finding and reporting DC as a state).
* constants.py: Lists of geos to produce, signals to produce, dataset ids, data source URL, etc.

#### Function stubs

If you have many functions you want to implement and/or anticipate a complex pipeline, consider starting with function stubs with comments or pseudo code. Bonus: consider writing unit tests upfront based on the expected behavior of each function.

Some stubs to consider: 

* Retrieve a list of filenames
* Download one data file (API call, csv reader, etc.)
* Iterate through filenames to download all data files
* Construct an SQL query
* Run an SQL query
* Keep a list of columns
* Geographic transformations (will tend to be wrappers around delphi_utils.geomapper functions)

Example stub:

```{python}
def api_call(args)
    #implement api call 
    return df 
```

Next, populate the function stubs with the intention of using them for a single pre-defined run (ignoring params.json, other geo levels, etc). If you fetched data programmatically in Stage 0, you can reuse that in your data-fetching code. If you reformatted data in Stage 1, you can reuse that too.
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

Make sure you have a functional environment with python 3.8.15+. For local runs, the makefile’s make install target will set up a local virtual environment with necessary packages.

(If working in R (not recommended), local runs can be run without a virtual environment or using the renv package, but production runs should be set up to user Docker.)

#### Dealing with data-types

* Often problem encountered prior to geomapper 
   * Problems that can arise and how to address them 
* Basic conversion

TODO: A list of assumptions that the server makes about various columns would be helpful. E.g. which geo values are allowed, should every valid date be present in some way, etc

#### Dealing with geos

In an ideal case, the data exists at one of our already covered geos:

* State: state_code or state_id
* FIPS (state+county codes)
* ZIP
* MSA (metro statistical area, int)
* HRR (hospital referral region, int)

If you want to map from one of these to another, we have a utility, geomapper.py, that covers most cases. A brief example of adding states with their population:

```{python}
from delphi_utils.geomap import GeoMapper
geo_mapper = GeoMapper()
geo_mapper.add_geocode(df, "state_id", "state_code", from_col = "state") # add codes and ids from the full names
df = geo_mapper.add_population_column(df, "state_code") # add state populations
hhs_version = geo_mapper.replace_geocode(df, "state_code","hhs", new_col = "geo_id") # aggregate to hhs regions, renaming the geo column to geo_id
```

This example is taken from hhs_hosp; more documentation can be found in the geomapper class definition.

#### Implement a Missing Value code system

The column is described here

#### Testing

As a general rule, it helps to decompose your functions into operations for which you can write unit tests. To run the tests, use make test in the base directory.

### Statistical Analysis

### Documentation


## Stage 3: Deployment

* This is after we have a working one-off script 
* Using Delphi utils and functionality 
* What happens to the data after it gets put in /receiving:

Next, the acquisition:covidcast component of the delphi-epidata codebase does the following immediately after an indicator run (You do need to set acquisition job up):

1. Look in the receiving/ folder to see if any new data files are available. If there are, then:
   1. Import the new data into the epimetric_full table of the epidata.covid database, filling in the columns as follows:
      1. source: parsed from the name of the subdirectory of receiving/
      2. signal: parsed from the filename
      3. time_type: parsed from the filename
      4. time_value: parsed from the filename
      5. geo_type: parsed from the filename
      6. geo_value: parsed from each row of the csv file
      7. value: parsed from each row of the csv file
      8. se: parsed from each row of the csv file
      9. sample_size: parsed from each row of the csv file
      10. issue: whatever now is in time_type units
      11. lag: the difference in time_type units from now to time_value
      12. value_updated_timestamp: now
   * Update the epimetric_latest table with any new keys or new versions of existing keys. 

### Staging

After developing the pipeline code, but before deploying in development, the pipeline should be run on staging for at least a week. This involves setting up some cronicle jobs as follows: first the indicator run

Then the acquisition run
https://cronicle-prod-01.delphi.cmu.edu/#Schedule?sub=edit_event&id=elr5clgy6rs
https://cronicle-prod-01.delphi.cmu.edu/#Schedule?sub=edit_event&id=elr5ctl7art

Note the staging hostname and how the acquisition job is chained to run right after the indicator job. Do a few test runs. 

If everything goes well (check staging db if data is ingested properly), make a prod version of the indicator run job and use that to run indicator on a daily basis.

Another thing to do is setting up the params.json template file in accordance with how you want to run the indicator and acquisition. Pay attention to the receiving directory, as well as how you can store credentials in vault. Refer to this guide for more vault info. 


### Signal Documentation

Apparently adding to a google spreadsheet, need to talk to someone (Carlyn) about the specifics

Github page signal documentation talk to Nat and Tina

## Appendix

Use the appendix to keep track of discussion and decisions

* New Archiver Procedure document about setting up an S3 ArchiveDiffer
* Indicator debugging document, somewhat out-of-date but might still be useful
