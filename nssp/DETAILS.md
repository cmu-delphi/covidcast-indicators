# NSSP data

We import the NSSP Emergency Department Visit data, including percentage and smoothed percentage of ER visits attributable to a given pathogen, from the CDC website. The data is provided at the county level, state level and national level; we do a population-weighted mean to aggregate from county data up to the HRR and MSA levels.

NSSP source data: https://data.cdc.gov/Public-Health-Surveillance/NSSP-Emergency-Department-Visit-Trajectories-by-St/rdmq-nq56/data_preview


## Geographical Levels
Primary source:
* `state`: reported from source using two-letter postal code
* `county`: reported from source using fips code
* `national`: just `us` for now, reported from source
* `hhs`, `hrr`, `msa`: not reported from source, so we computed them from county-level data using a weighted mean. Each county is assigned a weight equal to its population in the last census (2020).

## Metrics
*  `percent_visits_covid`, `percent_visits_rsv`, `percent_visits_influenza`: percentage of emergency department patient visits for specified pathogen.
*  `percent_visits_combined`: sum of the three percentages of visits for flu, rsv and covid.
*  `smoothed_percent_visits_covid`, `smoothed_percent_visits_rsv`, `smoothed_percent_visits_influenza`: 3 week moving average of the percentage of emergency department patient visits for specified pathogen.
*  `smoothed_percent_visits_combined`: 3 week moving average of the sum of the three percentages of visits for flu, rsv and covid.