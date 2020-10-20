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

You can see consistency checks and diffs with old sources in ./consistency_checks.ipynb

## Source files

- 03_20_MSAs.xls : [US Census Bureau](https://www.census.gov/geographies/reference-files/time-series/demo/metro-micro/delineation-files.html)
- 02_20_uszips.csv : Hand edited file from Jingjing, we only use the fips,zip encoding and also extract the states from these
- Crosswalk files from https://www.huduser.gov/portal/datasets/usps_crosswalk.html
- JHU crosswalk table: https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data#uid-lookup-table-logic
- ZIP/County population: https://www.census.gov/geographies/reference-files/time-series/geo/relationship-files.html#par_textimage_674173622, https://www2.census.gov/geo/docs/maps-data/data/rel/zcta_county_rel_10.txt?#

## Todo

- make direct cross tables for fips -> hrr and zip -> msa / state
- use hud for zip -> fips?

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

07/08/2020:
We are reserving 00001-00099 for states codes of the form 100XX where XX is the fips code for the state.  In the case that the CBSA codes change then it should be verified that these are not used.  The current smallest CBSA is 10100.

-James

07/22/2020:
- Introducing the COUNTY_ZIP and ZIP_COUNTY crosswalk files from https://www.huduser.gov/portal/datasets/usps_crosswalk.html
- Also the ZIP to HRR Crosswalk file (from 2018) from https://atlasdata.dartmouth.edu/static/supp_research_data
- Added the JHU crosswalk table and created a jhu_uid to fips crosswalk table: https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data#uid-lookup-table-logic

There are NaN fips in the JHU tables, so to resolve this we are moving over to using the JHU unique id.
We have to deal with the NaN fips by hand, which are 
```
748                                                   US
887                                        Recovered, US
888               Dukes and Nantucket, Massachusetts, US
889                            Kansas City, Missouri, US
890    Michigan Department of Corrections (MDOC), Mic...
891    Federal Correctional Institution (FCI), Michig...
892                           Air Force, US Military, US
893                                Army, US Military, US
894                        Marine Corps, US Military, US
895                                Navy, US Military, US
896                          Unassigned, US Military, US
897                                      US Military, US
898               Inmates, Federal Bureau of Prisons, US
899                 Staff, Federal Bureau of Prisons, US
900                        Federal Bureau of Prisons, US
901                                 Bear River, Utah, US
902                               Central Utah, Utah, US
903                             Southeast Utah, Utah, US
904                             Southwest Utah, Utah, US
905                                  TriCounty, Utah, US
906                               Weber-Morgan, Utah, US
907                                Veteran Hospitals, US
```
Is you look at geo_data.py::

08/04/2020:
Large changes in MSA from 2018 version from bea.gov (msa_list.csv), and the new 2020 version from census bureau (03_20_MSAs.xls).
Trying to use 2018 version instead from https://www.census.gov/geographies/reference-files/time-series/demo/metro-micro/delineation-files.html