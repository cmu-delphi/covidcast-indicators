# Quidel COVID Test

### Background
Starting May 9, 2020, we began getting Quidel COVID Test data and started reporting it from May 26, 2020 due to limitation in the data volume. The data contains a number of features for every test, including localization at 5-digit Zip Code level, a TestDate and StorageDate, patient age, and several identifiers that uniquely identify the device on which the test was performed (SofiaSerNum, the individual test (FluTestNum), and the result (ResultID). Multiple tests are stored on each device. The present Quidel COVID Test sensor concerns the positive rate in the test result.\\

### Signal names
- raw_pct_positive: estimates of the percentage of positive tests in total tests 
- smoothed_pct_positive: same as in (a), but where the estimates are formed by pooling together the last 7 days of data

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
se=1/100 * sqrt{ p*(1-p)/N } 
```
where we assume for each time point, the estimates follow a binomial distribution.


### Temporal Pooling
Additionally, as with the Quidel COVID Test signal, we consider smoothed estimates formed by pooling data over time. That is, daily, for each location, we first pool all data available in that location over the last 7 days, and we then recompute everything described in the last two subsections. Pooling in this data makes estimates available in more geographic areas, as many areas report very few tests per day, but have enough data to report when 7 days are considered. 