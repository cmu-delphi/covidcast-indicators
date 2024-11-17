# NSSP data

We import the NSSP Emergency Department Visit data, including percentage and smoothed percentage of ER visits attributable to a given pathogen, from the CDC website. The data is provided at the county level, state level and national level; we do a population-weighted mean to aggregate from county data up to the HRR and MSA levels.

There are 2 sources we grab data from for nssp:
- Primary source: https://data.cdc.gov/Public-Health-Surveillance/NSSP-Emergency-Department-Visit-Trajectories-by-St/rdmq-nq56/data_preview
- Secondary (2023RVR) source: https://data.cdc.gov/Public-Health-Surveillance/2023-Respiratory-Virus-Response-NSSP-Emergency-Dep/7mra-9cq9/data_preview
There are 8 signals output from the primary source and 4 output from secondary. There are no smoothed signals from secondary source.

Note that the data produced from secondary source are mostly the same as their primary source equivalent, with past analysis shows around 95% of datapoints having less than 0.1 value difference and the other 5% having a 0.1 to 1.2 value difference. 

## Geographical Levels
Primary source:
* `state`: reported from source using two-letter postal code
* `county`: reported from source using fips code
* `national`: just `us` for now, reported from source
* `hhs`, `hrr`, `msa`: not reported from source, so we computed them from county-level data using a weighted mean. Each county is assigned a weight equal to its population in the last census (2020).

Secondary (2023RVR) source:
* `state`: reported from source
* `hhs`: reported from source
* `national`: reported from source

## Metrics
*  `percent_visits_covid`, `percent_visits_rsv`, `percent_visits_influenza`: percentage of emergency department patient visits for specified pathogen.
*  `percent_visits_combined`: sum of the three percentages of visits for flu, rsv and covid.
*  `smoothed_percent_visits_covid`, `smoothed_percent_visits_rsv`, `smoothed_percent_visits_influenza`: 3 week moving average of the percentage of emergency department patient visits for specified pathogen.
*  `smoothed_percent_visits_combined`: 3 week moving average of the sum of the three percentages of visits for flu, rsv and covid.
*  `percent_visits_covid_2023RVR`, `percent_visits_rsv_2023RVR`, `percent_visits_influenza_2023RVR`: Taken from secondary source, percentage of emergency department patient visits for specified pathogen.
*  `percent_visits_combined_2023RVR`: Taken from secondary source, sum of the three percentages of visits for flu, rsv and covid.
