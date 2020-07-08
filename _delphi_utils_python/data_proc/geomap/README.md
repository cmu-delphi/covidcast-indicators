# Geocoding data processing pipeline

Authors: Jingjing Tang, James Sharpnack

The data_proc/geomap directory contains original source data, processing scripts, and notes for processing from original source to crosswalk tables in the data directory for the delphi_utils package.

## Usage

Requires the following source files below.

Run the following to write the cross files in the package data dir...
```
$ python geo_data_proc.py
```
this will build the following files...
- fips_msa_cross.csv
- zip_fips_cross.csv
- state_codes.csv


## Source files

1. 03_20_MSAs.xls : [US Census Bureau](https://www.census.gov/geographies/reference-files/time-series/demo/metro-micro/delineation-files.html)
2. 02_20_uszips.csv : Hand edited file from Jingjing, we only use the fips,zip encoding and also extract the states from these

## Todo 07/07/2020

- go through the trans files

## Notes

Some of the source files were constructed by hand, most notably 02_20_uszips.csv.

The 02_20_uszips.csv file is based on the newest consensus data including 5-digit zipcode, fips code, county name, state, population, HRR, HSA (I downloaded the original file from here https://simplemaps.com/data/us-zips. This file matches best to the most recent (2020) situation in terms of the population. But there still exist some matching problems. I manually checked and corrected those lines (~20) with zip-codes.com (https://www.zip-codes.com/zip-code/58439/zip-code-58439.asp). The mapping from 5-digit zipcode to HRR is based on the file in 2017 version downloaded from https://atlasdata.dartmouth.edu/static/supp_research_data

transStateToHRR.csv and transfipsToHRR.csv are used to transform data from state level or county level to HRR respectively. For example, x is the horizontal vector of covid cases for different states in 04/10/20, then we have x @ H = y, where H is the table provided in these two csv files and y is a horizontal vector of covid cases for different HRRs.

HRRs are represented by hrrnum. There are 306 hrrs in total. They are not named as consecutive numbers.

-Jingjing


04/14/20: 'msa_id' and 'msa_name' are added according to the msa_list.csv that Aaron found from https://apps.bea.gov/regional/docs/msalist.cfm (2019)   

04/15/20:
The newly updated(added columns) are based on cbsatocountycrosswalk.csv from https://data.nber.org/data/cbsa-fips-county-crosswalk.html
- 'msa' : MSA ID
- 'msaname': Name of the MSA
- 'cbsa': CBSA ID
- 'cbsaname': Name of the CBSA


04/19/20:
Changed to msa_list.csv again. 

05/20/20: Updated msa_list.csv to include MSAs in Puerto Rico, using the delineations file from March 2020: https://www.census.gov/geographies/reference-files/time-series/demo/metro-micro/delineation-files.html

06/15/20:
Added file co-est2019-annres.csv, which gives 2019 population estimates for each county by name 

Source: Annual Estimates of the Resident Population for Counties in the United States: April 1, 2010 to July 1, 2019 (CO-EST2019-ANNRES). U.S. Census Bureau, Population Division. Release Date: March 2020
Note: The estimates are based on the 2010 Census and reflect changes to the April 1, 2010 population due to the Count Question Resolution program and geographic program revisions. All geographic boundaries for the 2019 population estimates are as of January 1, 2019. For population estimates methodology statements, see http://www.census.gov/programs-surveys/popest/technical-documentation/methodology.html.

Note: The 6,222 people in Bedford city, Virginia, which was an independent city as of the 2010 Census, are not included in the April 1, 2010 Census enumerated population presented in the county estimates. In July 2013, the legal status of Bedford changed from a city to a town and it became dependent within (or part of) Bedford County, Virginia. This population of Bedford town is now included in the April 1, 2010 estimates base and all July 1 estimates for Bedford County. Because it is no longer an independent city, Bedford town is not listed in this table. As a result, the sum of the April 1, 2010 census values for Virginia counties and independent cities does not equal the 2010 Census count for Virginia, and the sum of April 1, 2010 census values for all counties and independent cities in the United States does not equal the 2010 Census count for the United States. Substantial geographic changes to counties can be found on the Census Bureau website at https://www.census.gov/programs-surveys/geography/technical-documentation/county-changes.html.


07/07/2020:
Introduced the March 2020 MSA file, source is [US Census Bureau](https://www.census.gov/geographies/reference-files/time-series/demo/metro-micro/delineation-files.html).  This file seems to differ in a few fips codes from the source for the 02_20_uszip file which Jingjing constructed.  There are at least 10 additional fips in 03_20_msa that are not in the uszip file, and one of the msa codes seems to be incorrect: 49020 (a google search confirms that it is incorrect in uszip and correct in the census data). 

-James
