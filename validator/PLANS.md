# Validator checks and features

## Current checks for indicator source data

* Missing dates within the selected range
* Recognized file name format
* Recognized geographical type (county, state, etc)
* Recognized geo id format (e.g. state is two lowercase letters)
* Geo id has been seen before, in historical data
* Missing geo type + signal + date combos based on the geo type + signal combos Covidcast metadata says should be available
* Missing ‘val’ values
* Negative ‘val’ values
* Out-of-range ‘val’ values (>0 for all signals, <=100 for percents, <=100 000 for proportions)
* Missing ‘se’ values
* Appropriate ‘se’ values, within a calculated reasonable range
* Stderr != 0
* If signal and stderr both = 0 (seen in Quidel data due to lack of Jeffreys correction, [issue 255](https://github.com/cmu-delphi/covidcast-indicators/issues/255#issuecomment-692196541))
* Missing ‘sample_size’ values
* Appropriate ‘sample_size’ values, ≥ 100 (default) or user-defined threshold
* Most recent date seen in source data is recent enough, < 1 day ago (default) or user-defined on a per-signal basis
* Most recent date seen in source data is not in the future
* Most recent date seen in source data is not older than most recent date seen in reference data
* Similar number of obs per day as recent API data (static threshold)
* Similar average value as API data (static threshold)
* Source data for specified date range is empty
* API data for specified date range is empty


## Current features

* Errors and warnings are summarized in class attribute and printed on exit
* If any non-suppressed errors are raised, the validation process exits with non-zero status
* Various check settings are controllable via indicator-specific params.json files
* User can manually disable specific checks for specific datasets using a field in the params.json file
* User can enable test mode (checks only a small number of data files) using a field in the params.json file

## Checks + features wishlist, and problems to think about

### Starter/small issues

* Check for duplicate rows
* Backfill problems, especially with JHU and USA Facts, where a change to old data results in a datapoint that doesn’t agree with surrounding data ([JHU examples](https://delphi-org.slack.com/archives/CF9G83ZJ9/p1600729151013900)) or is very different from the value it replaced. If date is already in the API, have any values changed significantly within the "backfill" window (use span_length setting). See [this](https://github.com/cmu-delphi/covidcast-indicators/pull/155#discussion_r504195207) for context.
* Run check_missing_date_files (or similar) on every geo type-signal type separately in comparative checks loop.

### Larger issues

* Check if [errors raised from validating all signals](https://docs.google.com/spreadsheets/d/1_aRBDrNeaI-3ZwuvkRNSZuZ2wfHJk6Bxj35Ol_XZ9yQ/edit#gid=1226266834) are correct, not false positives, not overly verbose or repetitive
* Check for erratic data sources that wrongly report all zeroes
  * E.g. the error with the Wisconsin data for the 10/26 forecasts
  * Wary of a purely static check for this
  * Are there any geo regions where this might cause false positives? E.g. small counties or MSAs, certain signals (deaths, since it's << cases)
  * This test is partially captured by checking avgs in source vs reference data, unless erroneous zeroes continue for more than a week
  * Also partially captured by outlier checking. If zeroes aren't outliers, then it's hard to say that they're erroneous at all.
* Easier suppression of many errors at once
* Nicer formatting for error “report”.
  * E.g. if a single type of error is raised for many different datasets, summarize all error messages into a single message? But it still has to be clear how to suppress each individually
* Use known erroneous/anomalous days of source data to tune static thresholds and test behavior
* If can't get data from API, do we want to use substitute data for the comparative checks instead?
  * E.g. most recent successful API pull -- might end up being a couple weeks older
  * Currently, any API fetch problems just doesn't do comparative checks at all.
* Improve performance and reduce runtime (no particular goal, just avoid being painfully slow!)
  * Profiling (iterate)
  * Check if saving intermediate files will improve efficiency (currently a bottleneck at "individual file checks" section. Parallelize?)
  * Make `all_frames` MultiIndex-ed by geo type and signal name? Make a dict of data indexed by geo type and signal name? May improve performance or may just make access more readable.
* Ensure validator runs on signals that require AWS credentials (iterate)

### Longer-term issues

* Data correctness and consistency over longer time periods (weeks to months). Compare data against long-ago (3 months?) API data for changes in trends.
  * Long-term trends and correlations between time series. Currently, checks only look at a data window of a few days
  * Any relevant anomaly detection packages already exist?
  * What sorts of hypothesis tests to use? See [time series trend analysis](https://www.genasis.cz/time-series/index.php?pg=home--trend-analysis).
  * See data-quality GitHub issues, Ryan’s [correlation notebook](https://github.com/cmu-delphi/covidcast/tree/main/R-notebooks), and Dmitry's [indicator validation notebook](https://github.com/cmu-delphi/covidcast-indicators/blob/deploy-jhu/testing_utils/indicator_validation.template.ipynb) for ideas
  * E.g. Doctor visits decreasing correlation with cases
  * E.g. WY/RI missing or very low compared to historical
* Use hypothesis testing p-values to decide when to raise error or not, instead of static thresholds. Many low but non-significant p-values will also raise error. See [here](https://delphi-org.slack.com/archives/CV1SYBC90/p1601307675021000?thread_ts=1600277030.103500&cid=CV1SYBC90) and [here](https://delphi-org.slack.com/archives/CV1SYBC90/p1600978037007500?thread_ts=1600277030.103500&cid=CV1SYBC90) for more background.
  * Order raised exceptions by p-value
  * Raise errors when one p-value (per geo region, e.g.) is significant OR when a bunch of p-values for that same type of test (different geo regions, e.g.) are "close" to significant
  * Correct p-values for multiple testing
  * Bonferroni would be easy but is sensitive to choice of "family" of tests; Benjamimi-Hochberg is a bit more involved but is less sensitive to choice of "family"; [comparison of the two](https://delphi-org.slack.com/archives/D01A9KNTPKL/p1603294915000500)
