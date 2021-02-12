# Safegraph Mobility Data

We import census block group-level raw mobility indicators from Safegraph,
calculate functions of the raw data, and then aggregate the data to the
county, state, HHS, and nation levels.

## Geographical Levels
* `county`: reported using zero-padded FIPS codes.  The FIPS codes are
  obtained by zero-padding the census block group codes and taking the first
  five digits, which are by construction the corresponding county FIPS code.
* `hhs`: reported using HHS region number 
* `state`: reported using two-letter postal code
* `nation`: reported using two-letter nation code. Just 'us' for now

## Metrics
* `completely_home_prop`, defined as:
		`completely_home_device_count / device_count`
* `full_time_work_prop`, defined as:
		`full_time_work_behavior_devices / device_count`
* `part_time_work_prop`, defined as:
		`part_time_work_behavior_devices / device_count`
* `median_home_dwell_time`

The raw mobility indicators are documented by
[Safegraph](https://docs.safegraph.com/docs/social-distancing-metrics).

After computing each metric on the census block group (CBG) level, we
aggregate to the county-level by taking the mean over CBGs in a county
to obtain the value and taking `sd / sqrt(n)` for the standard error, where
`sd` is the standard deviation over the metric values and `n` is the number
of CBGs in the county.  In doing so, we make the simplifying assumption
that each CBG contributes an iid observation to the county-level
distribution.  `n` also serves as the sample size.

## API Key

We access the Safegraph data using an AWS key-secret pair which is valid
until June 15, 2021.  The AWS credentials have been issued under
@huisaddison's Safegraph Data Catalog account.
