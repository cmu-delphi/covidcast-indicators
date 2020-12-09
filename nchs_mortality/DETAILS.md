# NCHS Mortality Data

We import the Mortality Data from NCHS website and export
the state-level data as-is in a weekly format.  

In order to avoid confusing public consumers of the data, we maintain
consistency how NCHS reports the data, please refer to [Exceptions](#Exceptions).

## Geographical Levels (`geo`)
* `state`: reported using two-letter postal code

## Metrics, Level 1 (`m1`)
* `deaths_covid_incidence`: All Deaths with confirmed or presumed COVID-19, 
                  coded to ICD–10 code U07.1
* `deaths_allcause_incidence`: Deaths from all causes.
* `deaths_percent_of_expected`:  the number of deaths for all causes for this 
                                 week in 2020 compared to the average number 
                                 across the same week in 2017–2019.
* `deaths_pneumonia_notflu_incidence`: Counts of deaths involving Pneumonia, with or without
                      COVID-19, excluding Influenza deaths(J12.0-J18.9).
* `deaths_covid_and_pneumonia_notflu_incidence`: Counts of deaths involving COVID-19 and Pneumonia,
                                excluding Influenza (U07.1 and J12.0-J18.9).
* `deaths_flu_incidence`: Counts of deaths involving Influenza, with or without 
                      COVID-19 or Pneumonia (J09-J11), includes COVID-19 or 
                      Pneumonia.
* `deaths_pneumonia_or_flu_or_covid_incidence`: Counts of deaths involving Pneumonia, 
                                            Influenza, or COVID-19, coded to ICD–10 
                                            codes U07.1 or J09–J18.9

Detailed descriptions are provided in the notes under Table 1 [here](https://www.cdc.gov/nchs/nvss/vsrr/COVID19/index.htm).

## Metrics, Level 2 (`m2`)
* `num`: number of new deaths on a given week
* `prop`: `num` / population * 100,000
* _**No** `m2` for signal `deaths_percent_of_expected`_.

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
refers to an epiweek). As defined by CDC, [epiweeks](https://wwwn.cdc.gov/nndss/document/MMWR_Week_overview.pdf) are seven days from Sunday to Saturday. We use Python package [epiweeks](https://pypi.org/project/epiweeks/) to convert the week-ending dates in the raw dataset into epiweek format.

### Data Versioning
Data versions are tracked on both a daily and weekly level.
On a daily level, we check for updates for NCHS mortality data every weekday as how it is reported by 
NCHS and stash these daily updates on S3, but not our API.
On a weekly level (on Mondays), we additionally upload the changes to the data 
made over the past week (due to backfill) to our API.
