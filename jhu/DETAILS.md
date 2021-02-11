# Johns Hopkins University Cases and Deaths

We import the confirmed case and deaths data from Johns Hopkins CSSE and export
the county-level data as-is.  We also aggregate the data to the MSA, HRR, HHS, 
State, and Nation levels.

In order to avoid confusing public consumers of the data, we maintain
consistency how JHU reports the data, please refer to [Exceptions](#Exceptions).

## Geographical Levels (`geo`)
* `county`: reported using zero-padded FIPS codes.  There are some exceptions
  that lead to inconsistency with the other COVIDcast data (but are necessary
  for internal consistency), noted below.  
* `msa`: reported using cbsa (consistent with all other COVIDcast sensors)
* `hrr`: reported using HRR number (consistent with all other COVIDcast sensors)
* `hhs`: reported using HHS region number 
* `state`: reported using two-letter postal code
* `nation`: reported using two-letter nation code. Just 'us' for now

## Metrics, Level 1 (`m1`)
* `confirmed`: Confirmed cases
* `deaths`

Recoveries are _not_ reported.

## Metrics, Level 2 (`m2`)
* `new_counts`: number of new {confirmed cases, deaths} on a given day
* `cumulative_counts`: total number of {confirmed cases, deaths} up until the
  first day of data (January 22nd)
* `incidence`: `new_counts` / population * 100000

All three `m2` are ultimately derived from `cumulative_counts`, which is first
available on January 22nd.  In constructing `new_counts`, we take the first
discrete difference of `cumulative_counts`,  and assume that the
`cumulative_counts` for January 21st is uniformly zero.  This should not be a
problem, because there there is only one county with a nonzero
`cumulative_count` on January 22nd, with a value of 1.

For deriving `incidence`, we use the estimated 2019 county population estimates
from the [US Census Bureau](https://www.census.gov/data/tables/time-series/demo/popest/2010s-counties-total.html).

## Exceptions

To prevent confusing public consumers of the data, we report the data as closely
as possible to the way JHU reports their data, using the same County FIPS codes.
Nonetheless, there are a few exceptions which should be of interest to the
visualization and modeling teams. These exceptions can be found at the [JHU Delphi
Epidata API documentation page](https://cmu-delphi.github.io/delphi-epidata/api/covidcast-signals/jhu-csse.html#geographical-exceptions).

## Negative incidence

Negative incidence is possible because figures are sometimes revised
downwards, e.g., when a public health authority moves cases from County X
to County Y, County X may have negative incidence.

## Non-integral counts

Because the MSA and HRR numbers are computed by taking population-weighted
averages, the count data at those geographical levels may be non-integral.
