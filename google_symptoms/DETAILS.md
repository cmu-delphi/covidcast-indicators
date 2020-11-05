# Google Symptoms

We import the normalized symptom search term popularity data from the Google 
Research's Open COVID-19 Data project and export the county-level and state-level 
data as-is.  We also aggregate the data to the MSA and HRR levels.

## Geographical Levels (`geo`)
* `county`: reported using zero-padded FIPS codes.  The county level data is derived 
from `/subregions/state/2020_US_state_daily_symptoms_dataset.csv`.
* `msa`: reported using cbsa (consistent with all other COVIDcast sensors). The msa
level data is derived from county level data using population weighted average.
* `hrr`: reported using HRR number (consistent with all other COVIDcast sensors). The 
hrr level data is derived from county level data using population weighted average.
* `state`: reported using two-letter postal code. The state level data is derived from
`2020_US_daily_symptoms_dataset.csv` which includes data for District of Columbia.

## Metrics, Level 1 (`m1`)
* `anosmia`: Google search volume for Anosmia-related searches
* `ageusia`: Google search volume for Ageusia-related searches
*`combined_symptoms`*: The sum of Google search volume for Anosmia-related searches and  Ageusia-related searches.

## Metrics, Level 2 (`m2`)
* `raw_search`:  Google search volume reported as-is
* `smoothed_search`:  Google search volume using 7-day moving average

This data reflects the volume of Google searches mapped to symptoms such Anosmia
and Ageusia. The resulting daily dataset for each region showing the relative frequency
of searches for each symptom.  This signal is measured in arbitrary units that are normalized
for population and for the most popular symptom search term within a geographic region. Thus, 
values are not comparable between geographic regions. Larger numbers represent higher 
numbers of symptom-related searches.