# Geocoding Data Processing

Authors: Jingjing Tang, James Sharpnack, Dmitry Shemetov

## Usage

Requires the following source files below.

Run the following to build the crosswalk tables in `covidcast-indicators/_delph_utils_python/delph_utils/data`

```sh
$ python geo_data_proc.py
```

Find data consistency checks in `./source-file-sanity-check.ipynb`.

## Geo Codes

We support the following geocodes.

- The [ZIP code](https://en.wikipedia.org/wiki/ZIP_Code) is a US postal code used by the USPS and the [FIPS code](https://en.wikipedia.org/wiki/FIPS_county_code) is an identifier for US counties and other associated territories. The ZIP code is five digit code (with leading zeros).
- The FIPS code is a five digit code (with leading zeros), where the first two digits are a two-digit state code and the last three are a three-digit county code (see this [US Census Bureau page](https://www.census.gov/library/reference/code-lists/ansi.html) for detailed information).
- The Metropolitan Statistical Area (MSA) code refers to regions around cities (these are sometimes referred to as CBSA codes). More information on these can be found at the [US Census Bureau](https://www.census.gov/programs-surveys/metro-micro/about.html). We rserve 10001-10099 for states codes of the form 100XX where XX is the FIPS code for the state (the current smallest CBSA is 10100). In the case that the CBSA codes change then it should be verified that these are not used.
- State codes are a series of equivalent identifiers for US state. They include the state name, the state number (state_id), and the state two-letter abbreviation (state_code). The state number is the state FIPS code. See [here](https://en.wikipedia.org/wiki/List_of_U.S._state_and_territory_abbreviations) for more.
- The Hospital Referral Region (HRR) and the Hospital Service Area (HSA). More information [here](https://www.dartmouthatlas.org/covid-19/hrr-mapping/).

## Source Files

The source files are requested from a government URL when `geo_data_proc.py` is run (see the top of said script for the URLs). Below we describe the locations to find updated versions of the source files, if they are ever needed.

- ZIP -> FIPS (county) population tables available from [US Census](https://www.census.gov/geographies/reference-files/time-series/geo/relationship-files.html#par_textimage_674173622). This file contains the population of the intersections between ZIP and FIPS regions, allowing the creation of a population-weighted transform between the two. As of 4 February 2022, this source did not include population information for 24 ZIPs that appear in our indicators. We have added those values manually using information available from the [zipdatamaps website](www.zipdatamaps.com).
- ZIP -> HRR -> HSA crosswalk file comes from the 2018 version at the [Dartmouth Atlas Project](https://atlasdata.dartmouth.edu/static/supp_research_data).
- FIPS -> MSA crosswalk file comes from the September 2018 version of the delineation files at the [US Census Bureau](https://www.census.gov/geographies/reference-files/time-series/demo/metro-micro/delineation-files.html).
- State Code -> State ID -> State Name comes from the ANSI standard at the [US Census](https://www.census.gov/library/reference/code-lists/ansi.html#par_textimage_3).

## Derived Files

The rest of the crosswalk tables are derived from the mappings above. We provide crosswalk functions from granular to coarser codes, but not the other way around. This is because there is no information gained when crosswalking from coarse to granular.

## Deprecated Source Files

- ZIP to FIPS to HRR to states: `02_20_uszips.csv` comes from a version of the table [here](https://simplemaps.com/data/us-zips) modified by Jingjing to include population weights.
  - The `02_20_uszips.csv` file is based on the newest consensus data including 5-digit zipcode, fips code, county name, state, population, HRR, HSA (I downloaded the original file from [here](https://simplemaps.com/data/us-zips). This file matches best to the most recent (2020) situation in terms of the population. But there still exist some matching problems. I manually checked and corrected those lines (~20) with [zip-codes](https://www.zip-codes.com/zip-code/58439/zip-code-58439.asp). The mapping from 5-digit zipcode to HRR is based on the file in 2017 version downloaded from [here](https://atlasdata.dartmouth.edu/static/supp_research_data).
- ZIP -> FIPS is provided by [huduser.gov](https://www.huduser.gov/portal/datasets/usps_crosswalk.html) for zip -> fips?
- FIPS county population data from [US Census Bureau](http://www.census.gov/programs-surveys/popest/technical-documentation/methodology.html). Details of Bedford, Virginia counting [here](https://www.census.gov/programs-surveys/geography/technical-documentation/county-changes.html).
- CBSA -> FIPS crosswalk from [here](https://data.nber.org/data/cbsa-fips-county-crosswalk.html) (the file is `cbsatocountycrosswalk.csv`).
- MSA tables from March 2020 [here](https://www.census.gov/geographies/reference-files/time-series/demo/metro-micro/delineation-files.html). This file seems to differ in a few fips codes from the source for the 02_20_uszip file which Jingjing constructed. There are at least 10 additional fips in 03_20_msa that are not in the uszip file, and one of the msa codes seems to be incorrect: 49020 (a google search confirms that it is incorrect in uszip and correct in the census data).
- MSA tables from 2019 [here](https://apps.bea.gov/regional/docs/msalist.cfm)
