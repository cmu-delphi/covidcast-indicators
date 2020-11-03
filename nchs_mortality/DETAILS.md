# NCHS Mortality Data

We import the NCHS Mortality Data from CDC website and export
the state-level data as-is in a weekly format.  

In order to avoid confusing public consumers of the data, we maintain
consistency how NCHS reports the data, please refer to [Exceptions](#Exceptions).

## Geographical Levels (`geo`)
* `state`: reported using two-letter postal code

## Metrics, Level 1 (`m1`)
* `covid_deaths`: All Deaths with confirmed or presumed COVID-19, 
                  coded to ICD–10 code U07.1
* `total_deaths`: Deaths from all causes.
* `percent_of_expected_deaths`:  the number of deaths for all causes for this 
                                 week in 2020 compared to the average number 
                                 across the same week in 2017–2019.
* `pneumonia_deaths`: Counts of deaths involving Pneumonia, with or without
                      COVID-19, excluding Influenza deaths(J12.0-J18.9).
* `pneumonia_and_covid_deaths`: Counts of deaths involving COVID-19 and Pneumonia,
                                excluding Influenza (U07.1 and J12.0-J18.9).
* `influenza_deaths`: Counts of deaths involving Influenza, with or without 
                      COVID-19 or Pneumonia (J09-J11), includes COVID-19 or 
                      Pneumonia.
* `pneumonia_influenza_or_covid_19_deaths`: Counts of deaths involving Pneumonia, 
                                            Influenza, or COVID-19, coded to ICD–10 
                                            codes U07.1 or J09–J18.9

Detailed descriptions are provided in the notes under Table 1 [here](https://www.cdc.gov/nchs/nvss/vsrr/COVID19/index.htm).

## Metrics, Level 2 (`m2`)
* `num`: number of new deaths on a given week
* `prop`: `num` / population * 100,000
* _**No** `m2` for signal `percent_of_expected_deaths`._

## Exceptions

At the State level, we report the data _exactly_ as NCHS reports their
mortality data, to prevent confusing public consumers of the data.
The visualization and modeling teams should take note of these exceptions.

### New York City

New York City is considered as a special state in the NCHS Mortality data,
but we don't consider NYC separately. The death counts for NYC would be included
 in New York State in our reports.

### Report Using Epiweeks 

We report the NCHS Mortality data in a weekly format (`weekly_YYYYWW`, where `YYYYWW`
refers to an epiweek). However, NCHS reports their weekly data from Saturday to 
Saturday. We assume there is a one day shift. For example, they report a death counts 
for Alaska in a week starting from date D, we will report the timestamp of this report 
as the corresponding epiweek of date(D + 1).

### Data Versioning
Data versions are tracked on both a daily and weekly level.
On a daily level, we check for updates for NCHS mortality data every weekday as how it is reported by 
CDC and stash these daily updates on S3, but not our API.
On a weekly level (on Mondays), we additionally upload the changes to the data 
made over the past week (due to backfill) to our API.
