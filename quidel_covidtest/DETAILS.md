# Quidel COVID Test

### Background
Starting May 9, 2020, we began getting Quidel COVID Test data and started reporting it from May 26, 2020 due to limitation in the data volume. The data contains a number of features for every test, including localization at 5-digit Zip Code level, a TestDate and StorageDate, patient age, and several identifiers that uniquely identify the device on which the test was performed (SofiaSerNum, the individual test (FluTestNum), and the result (ResultID). Multiple tests are stored on each device. The present Quidel COVID Test sensor concerns the positive rate in the test result.

### Signal names
- covid_ag_raw_pct_positive: percent of tests returning positive that day
- covid_ag_smoothed_pct_positive: same as above, but for the moving average of the most recent 7 days

### Estimating percent positive test proportion
Let n be the number of total COVID tests taken over a given time period and a given location (the test result can be negative/positive/invalid). Let x be the number of tests taken with positive results in this location over the given time period. We are interested in estimating the percentage of positive tests which is defined as:
```
p = 100 * x / n 
```
We estimate p across 3 temporal-spatial aggregation schemes:
- daily, at the MSA (metropolitan statistical area) level;
- daily, at the HRR (hospital referral region) level;
- daily, at the state level.
We are able to make these aggregations accurately because each test is reported with its 5-digit ZIP code. We do not report estimates for individual counties, as typically each county has too few tests to make the estimated value statistically meaningful.

**MSA and HRR levels**: In a given MSA or HRR, suppose N flu tests are taken in a certain time period, X is the number of tests taken with positive results. If N >= 50, we simply use:
```
p = 100 * X / N 
```
If N < 50, we lend 50 - N  fake samples from its home state to shrink the estimate to the state's mean, which means:
```
p = 100 * [ N /50 * X/N + (50 - N)/50  * Xs /Ns ] 
```
where Ns, Xs are the number of flu tests and the number of flu tests taken with positive results taken in its home state in the same time period.

**State level**:  the states with sample sizes smaller than a certain threshold are discarded. (The threshold is set to be 50 temporarily). For the rest of the states with big enough sample sizes,
```
p = 100 * X / N
```

The estimated standard error is simply:
```
se = 100 * sqrt{ p/100 *(1-p/100)/N } 
```
where we assume for each time point, the estimates follow a binomial distribution.


### Temporal and Spatial Pooling
We conduct temporal and spatial pooling for the smoothed signal. The spatial pooling is described in the previous section where we shrink the estimates to the state's mean if the total test number is smaller than 50 for a certain location on a certain day. Additionally, as with the Quidel COVID Test signal, we consider smoothed estimates formed by pooling data over time. That is, daily, for each location, we first pool all data available in that location over the last 7 days, and we then recompute everything described in the last two subsections. Pooling in this data makes estimates available in more geographic areas.

### Exceptions
There are 9 special zip codes that are included in Quidel COVID raw data but are not included in our reports temporarily since we do not have enough mapping information for them. 

|zip       |State| Number of Tests|
|---|-------|------|
|78086    |TX|98|
|20174    | VA|17|
|48824    |MI|14|
|32313     |FL|37|
|29486    |SC|69|
|75033    |TX|2318|
|79430    |TX|43|
|44325 |OH|56|
|75072    |TX|63|

* Number of tests calculated until 08-05-2020
* Until 08-05-2020, only 2,715 tests out of 942,293 tests for those zip codes. 
