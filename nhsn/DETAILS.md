# NHSN data

We import the NHSN Weekly Hospital Respiratory Data

There are 2 sources we grab data from for nhsn:

Primary source: https://data.cdc.gov/Public-Health-Surveillance/Weekly-Hospital-Respiratory-Data-HRD-Metrics-by-Ju/ua7e-t2fy/about_data
Secondary (preliminary source): https://data.cdc.gov/Public-Health-Surveillance/Weekly-Hospital-Respiratory-Data-HRD-Metrics-by-Ju/mpgq-jmmr/about_data

## Geographical Levels
* `state`: reported using two-letter postal code
* `national`: just `us` for now

## Metrics
*  `confirmed_admissions_covid`: total number of confirmed admission for covid
*  `confirmed_admissions_flu`: total number of confirmed admission for flu
*  `prelim_confirmed_admissions_covid`: total number of confirmed admission for covid from preliminary source
*  `prelim_confirmed_admissions_flu`: total number of confirmed admission for flu  from preliminary source
