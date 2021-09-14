# Weekly Patterns Dataset in SafeGraph Mobility Data

We import Zip Code-level raw mobility indicators from SafeGraph **Weekly 
Patterns** dataset, calculate functions of the raw data, and then aggregate 
he data to the county, hrr, msa, hhs, nation, and state levels.

## Brand Information
SafeGraph provides daily number of visits to points of interest (POIs) in Weekly
Patterns datasets which is documented [here](https://docs.safegraph.com/docs/weekly-patterns).
Base information such as location name, address, category, and brand association 
for POIs are provided in **Places Schema** dataset which is documented [here]
(https://docs.safegraph.com/docs/places-schema). SafeGraph does not update their
list of POIs frequently but there does exist versioning issue. The release 
version can be found in `release-metadata` in Weekly Patterns dataset and there
are corresponding `brand_info.csv` provided in Places Schema dataset. To save 
storage space, we do not download the whole Places Schema dataset, but only add 
new necessary `brand_info.csv` in `./statics` with suffix YYYYMM(release version).


Example procedure for adding a new brand info file:

```{bash}
$ aws --profile safegraph s3 cp s3://sg-c19-response/weekly-patterns-delivery-2020-12/release-2021-07/weekly/release_metadata/2021/07/21/18/release_metadata.csv - --endpoint https://s3.wasabisys.com
metadata_description,metadata_value
core_places_version_used,06-2021
total_poi,4456874
total_branded_poi,980083
$ aws --profile safegraph s3 ls s3://sg-c19-response/core-places-delivery/brand_info/2021/06/ --endpoint https://s3.wasabisys.com --recursive | grep brand_info.csv
2021-06-04 20:44:04     952825 core-places-delivery/brand_info/2021/06/05/00/brand_info.csv
$ aws --profile safegraph s3 cp s3://sg-c19-response/core-places-delivery/brand_info/2021/06/05/00/brand_info.csv ./static/brand_info/brand_info_202106.csv --endpoint https://s3.wasabisys.com
download: s3://sg-c19-response/core-places-delivery/brand_info/2021/06/05/00/brand_info.csv to static/brand_info/brand_info_202106.csv
```

Example `~/.aws/config` to run the above:

```
[profile safegraph]
output = json
region = us-east-1
aws_access_key_id = {{ safegraph_aws_access_key_id }}
aws_secret_access_key = {{ safegraph_aws_secret_access_key }}
```

## Geographical Levels
* `county`: reported using zero-padded FIPS codes (consistency with the 
            other COVIDcast data)
* `msa`: reported using CBSA (consistent with all other COVIDcast sensors)
* `hrr`: reported using HRR number (consistent with all other COVIDcast sensors)
* `state`: reported using two-letter postal code
* `hhs`: reported using HHS region number
* `nation`: reported using two-letter country abbreviation. Just `us` for now.

## Metrics,  Level 1 (`m1`)
* `bars_visit`: The number of visits to bars(places with NAICS code = 722410)
* `restaurants_visit`: The number of visits to restaurants(places with NAICS 
                            code = 722511)

## Metrics, Level 2 (`m2`)
* `num`: number of visits in a given week
* `prop`: `num` / population * 100,000 (Notice the population here only includes 
population aggregated at Zip Code level. If there are no POIs for a certain 
Zip Code, the population there won't be considered.)


## API Key

We access the SafeGraph data using an AWS key-secret pair which is valid
until June 15, 2021.  The AWS credentials have been issued under
@huisaddison's SafeGraph Data Catalog account.
