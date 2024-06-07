# NWSS wastewater data

We import the wastewater data, including percentile, raw counts, and smoothed data, from the CDC website, aggregate to the state level from the sub-county wastewater treatment plant level, and export the aggregated data.

For the mean time, we only export the state-level aggregations of the data. This includes aggregating cities into their respective states.
Ideally we will export the state level, the county level, and the wastewater treatment plant level. Possibly an exact mirror that includes sample sites as well.
## Geographical Levels
* `state`: reported using two-letter postal code
## Metrics
*  `percentile`: This metric shows whether SARS-CoV-2 virus levels at a site are currently higher or lower than past historical levels at the same site. 0% means levels are the lowest they have been at the site; 100% means levels are the highest they have been at the site. Public health officials watch for increasing levels of the virus in wastewater over time and use this data to help make public health decisions. 
*  `ptc_15d`: The percent change in SARS-CoV-2 RNA levels over the 15-day interval defined by 'date_start' and 'date_end'.
   Percent change is calculated as the modeled change over the interval, based on linear regression of log-transformed SARS-CoV-2 levels.
   SARS-CoV-2 RNA levels are wastewater concentrations that have been normalized for wastewater composition.
*  `detect_prop_15d`: The proportion of tests with SARS-CoV-2 detected, meaning a cycle threshold (Ct) value <40 for RT-qPCR or at least 3 positive droplets/partitions for RT-ddPCR, by sewershed over the 15-day window defined by 'date_start' and "date_end'. The detection proportion is the percent calculated by dividing the 15-day rolling sum of SARS-CoV-2 detections by the 15-day rolling sum of the number of tests for each sewershed and multiplying by 100.
