# NHSN data

We import the NHSN Weekly Hospital Respiratory Data

There are 2 sources we grab data from for nhsn:
Note that they are from the same source, but with different cadence and one reporting preliminary data for the previous reporting week 

Primary source: https://data.cdc.gov/Public-Health-Surveillance/Weekly-Hospital-Respiratory-Data-HRD-Metrics-by-Ju/ua7e-t2fy/about_data
Secondary (preliminary source): https://data.cdc.gov/Public-Health-Surveillance/Weekly-Hospital-Respiratory-Data-HRD-Metrics-by-Ju/mpgq-jmmr/about_data

## Geographical Levels
* `state`: reported using two-letter postal code
* `national`: just `us` for now
* `hhs`: reporting using Geomapper with state level 

## Metrics
*  `confirmed_admissions_covid`: total number of confirmed admission for covid
*  `confirmed_admissions_flu`: total number of confirmed admission for flu
*  `prelim_confirmed_admissions_covid`: total number of confirmed admission for covid from preliminary source
*  `prelim_confirmed_admissions_flu`: total number of confirmed admission for flu  from preliminary source

## Additional Notes
HHS dataset and NHSN dataset covers the equivalent data of hospital admission for covid and flu.
As a general trend, HHS and NHSN data matches pretty well.
However, there are differences between some of the states, notably for GA (untill 2023), LA, NV, PR (late 2020-early 2021), TN all have HHS substantially lower, HHS is substantially lower than NHSN.

Some states have this spike in NHSN or hhs where the other source doesn't have a spike and spikes don't happen at the same time_values across states

More details regarding the analysis is available in the [analysis.ipynb](notebook%2Fanalysis.ipynb)
(may require installing additional packages to work)