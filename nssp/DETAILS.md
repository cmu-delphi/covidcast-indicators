# NSSP data

We import the NSSP Emergency Department Visit data, including percentage and smoothed percentage data, from the CDC website. The data is available in county level, state level and national level.

## Geographical Levels
* `state`: reported using two-letter postal code
* `county`: reported using fips code
* `national`: just `us` for now
## Metrics
*  `percent_visits_covid`, `percent_visits_rsv`, `percent_visits_influenza`: percentage of emergency department patient visits for specified pathogen.
*  `percent_visits_combined`: sum of the three percentages of visits for flu, rsv and covid.
*  `smoothed_percent_visits_covid`, `smoothed_percent_visits_rsv`, `smoothed_percent_visits_influenza`: Smoothed percentage of emergency department patient visits for specified pathogen.
*  `smoothed_percent_visits_combined`: Smoothed sum of the three percentages of visits for flu, rsv and covid.