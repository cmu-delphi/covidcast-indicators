# NSSP data

We import the NSSP Emergency Department Visit data, including percentage and smoothed percentage of ER visits attributable to a given pathogen, from the CDC website. The data is provided at the county level, state level and national level; we do a population-weighted mean to aggregate from county data up to the HRR and MSA levels.

There are 2 sources we grab data from for nssp:
- Primary source: https://data.cdc.gov/Public-Health-Surveillance/NSSP-Emergency-Department-Visit-Trajectories-by-St/rdmq-nq56/data_preview
- Secondary source: https://data.cdc.gov/Public-Health-Surveillance/2023-Respiratory-Virus-Response-NSSP-Emergency-Dep/7mra-9cq9/data_preview
There are 8 signals output from the primary source and 4 output from secondary. Secondary source data is only available from state-level geos and above, though secondary data might be updated more often.

## Geographical Levels
* `state`: reported using two-letter postal code
* `county`: reported using fips code
* `national`: just `us` for now
## Metrics
*  `percent_visits_covid`, `percent_visits_rsv`, `percent_visits_influenza`: percentage of emergency department patient visits for specified pathogen.
*  `percent_visits_combined`: sum of the three percentages of visits for flu, rsv and covid.
*  `smoothed_percent_visits_covid`, `smoothed_percent_visits_rsv`, `smoothed_percent_visits_influenza`: 3 week moving average of the percentage of emergency department patient visits for specified pathogen.
*  `smoothed_percent_visits_combined`: 3 week moving average of the sum of the three percentages of visits for flu, rsv and covid.
*  `percent_visits_covid_secondary`, `percent_visits_rsv_secondary`, `percent_visits_influenza_secondary`: Taken from secondary source, percentage of emergency department patient visits for specified pathogen.
*  `percent_visits_combined_secondary`: Taken from secondary source, sum of the three percentages of visits for flu, rsv and covid.