# Patterns Dataset in Safegraph Mobility Data

We import Zip Code-level raw mobility indicators from Safegraph **Weekly 
Patterns** dataset, calculate functions of the raw data, and then aggregate 
he data to the county, hrr, msa and state levels.

## Brand Information
Safegraph provides daily number of visits to points of interest (POIs) in Weekly
Patterns datasets which is documanted [here](https://docs.safegraph.com/docs/weekly-patterns).
Base information such as location name, address, category, and brand association 
for POIs are provided in **Places Schema** dataset which is documented [here]
(https://docs.safegraph.com/docs/places-schema). Safegraph do not update there
list of POIs frequently but there does exist versioning issue. The release 
version can be found in `release-metadata` in Weekly Patterns dataset and there
are correspounding `brand_info.csv` provided in Places Schema dataset. To save 
storage space, we won't donwload the whole Places Schema dataset, but only add 
new necesary `brand_info.csv` in `./statics` with suffix YYYYMM(release version).

## Geographical Levels
* `county`: reported using zero-padded FIPS codes (consistency with the 
            other COVIDcast data)
* `msa`: reported using cbsa (consistent with all other COVIDcast sensors)
* `hrr`: reported using HRR number (consistent with all other COVIDcast sensors)
* `state`: reported using two-letter postal code

## Metrics,  Level 1 (`m1`)
* `bars_visit`: The number of visits to bars(places with naics code = 722410)
* `restaurants_visit`: The number of visits to restaurants(places with naics 
                            code = 722511)

## Metrics, Level 2 (`m2`)
* `num`: number of new deaths on a given week
* `prop`: `num` / population * 100,000 (Notice the population here only includes 
population aggregated at Zip Code level. If there are no POIs for a certain 
Zip Code, the population there won't be considered.)


## API Key

We access the Safegraph data using an AWS key-secret pair which is valid
until June 15, 2021.  The AWS credentials have been issued under
@huisaddison's Safegraph Data Catalog account.
