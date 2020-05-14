# Johns Hopkins University Cases and Deaths

We import the confirmed case and deaths data from Johns Hopkins CSSE and export
the county-level data as-is.  We also aggregate the data to the MSA, HRR, and
State levels.

In order to avoid confusing public consumers of the data, we maintain
consistency how JHU reports the data, please refer to [Exceptions](#Exceptions).

## Geographical Levels (`geo`)
* `county`: reported using zero-padded FIPS codes.  There are some exceptions
  that lead to inconsistency with the other COVIDcast data (but are necessary
  for internal consistency), noted below.  
* `msa`: reported using cbsa (consistent with all other COVIDcast sensors)
* `hrr`: reported using HRR number (consistent with all other COVIDcast sensors)
* `state`: reported using two-letter postal code

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

For deriving `incidence`, we use the estimated 2019 county population values
from the US Census Bureau.  https://www.census.gov/data/tables/time-series/demo/popest/2010s-counties-total.html

## Exceptions

At the County (FIPS) level, we report the data _exactly_ as JHU reports their
data, to prevent confusing public consumers of the data.
The visualization and modeling teams should take note of these exceptions.

### New York City

New York City comprises of five boroughs:

|Borough Name       |County Name        |FIPS Code      |
|-------------------|-------------------|---------------|
|Manhattan          |New York County    |36061          |
|The Bronx          |Bronx County       |36005          |
|Brooklyn           |Kings County       |36047          |
|Queens             |Queens County      |36081          |
|Staten Island      |Richmond County    |36085          |

**Data from all five boroughs are reported under New York County,
FIPS Code 36061.**  The other four boroughs are included in the dataset
and show up in our API, but they should be uniformly zero.

All NYC counts are mapped to the MSA with CBSA ID 35620, which encompasses
all five boroughs.  All NYC counts are mapped to HRR 303, which intersects
all five boroughs (297 also intersects the Bronx, 301 also intersects
Brooklyn and Queens, but absent additional information, I am leaving all
counts in 303).

### Kansas City, Missouri

Kansas City intersects the following four counties, which themselves report
confirmed case and deaths data:

|County Name        |FIPS Code      |
|-------------------|---------------|
|Jackson County     |29095          |
|Platte County      |29165          |
|Cass County        |29037          |
|Clay County        |29047          |

**Data from Kansas City is given its own dedicated line, with FIPS
code 70003.**  This is how JHU encodes their data.  However, the data in
the four counties that Kansas City intersects is not necessarily zero.

For the mapping to HRR and MSA, the counts for Kansas City are dispersed to
these four counties in equal proportions.

### Dukes and Nantucket Counties, Massachusetts

**The counties of Dukes and Nantucket report their figures together,
and we (like JHU) list them under FIPS Code 70002.**  Here are the FIPS codes
for the individual counties:

|County Name        |FIPS Code      |
|-------------------|---------------|
|Dukes County       |25007          |
|Nantucket County   |25019          |

For the mapping to HRR and MSA, the counts for Dukes and Nantucket are
dispersed to the two counties in equal proportions.

The data in the individual counties is expected to be zero.

### Mismatched FIPS Codes

Finally, there are two FIPS codes that were changed in 2015, leading to
mismatch between us and JHU.  We report the data using the FIPS code used
by JHU, again to promote consistency and avoid confusion by external users
of the dataset.  For the mapping to MSA, HRR, these two counties are
included properly.

|County Name        |State          |"Our" FIPS         |JHU FIPS       |
|-------------------|---------------|-------------------|---------------|
|Oglala Lakota      |South Dakota   |46113              |46102          |
|Kusilvak           |Alaska         |02270              |02158          |

Documentation for the changes made by the US Census Bureau in 2015:
https://www.census.gov/programs-surveys/geography/technical-documentation/county-changes.html

## Negative incidence

Negative incidence is possible because figures are sometimes revised
downwards, e.g., when a public health authority moves cases from County X
to County Y, County X may have negative incidence.

## Non-integral counts

Because the MSA and HRR numbers are computed by taking population-weighted
averages, the count data at those geographical levels may be non-integral.

## Counties not in our canonical dataset

Some FIPS codes do not appear as the primary FIPS for any ZIP code in our
canonical `02_20_uszips.csv`; they appear in the `county` exported files, but
for the MSA/HRR mapping, we disburse them equally to the counties with whom
they appear as a secondary FIPS code.  The identification of such "secondary"
FIPS codes are documented in `notebooks/create-mappings.ipynb`.  The full list
of `secondary, [mapped]` is:

```
SECONDARY_FIPS = [   # generated by notebooks/create-mappings.ipynb
	('51620', ['51093', '51175']),
	('51685', ['51153']),
	('28039', ['28059', '28041', '28131', '28045', '28059', '28109',
                    '28047']),
	('51690', ['51089', '51067']),
	('51595', ['51081', '51025', '51175', '51183']),
	('51600', ['51059', '51059', '51059']),
	('51580', ['51005']),
	('51678', ['51163']),
    ]
```
