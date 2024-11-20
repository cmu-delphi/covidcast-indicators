# NHSN data

We import the NHSN Weekly Hospital Respiratory Data

There are 2 sources we grab data from for nhsn:

Primary source: https://data.cdc.gov/Public-Health-Surveillance/Weekly-Hospital-Respiratory-Data-HRD-Metrics-by-Ju/ua7e-t2fy/about_data
Secondary (preliminary source): https://data.cdc.gov/Public-Health-Surveillance/Weekly-Hospital-Respiratory-Data-HRD-Metrics-by-Ju/mpgq-jmmr/about_data

## Geographical Levels
* `state`: reported using two-letter postal code
* `national`: just `us` for now

## Metrics
*  `confirmed_admissions_covid`: percentage of emergency department patient visits for specified pathogen.
*  `confirmed_admissions_flu`: sum of the three percentages of visits for flu, rsv and covid.