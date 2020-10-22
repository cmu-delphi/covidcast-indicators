# Validator checks and features

## Current checks for indicator source data

* Missing dates within the selected range
* Recognized file name format
* Recognized geographical type (county, state, etc)
* Recognized geo id format (e.g. state is two lowercase letters)
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


## Current features

* Errors are summarized in class attribute and printed on exit
* If any non-suppressed errors are raised, the validation process exits with non-zero status
* Various check settings are controllable via indicator-specific params.json files
* User can manually disable certain checks for certain sets of data using a field in the params.json file
* User can enable test mode (checks only a small number of data files) using a field in the params.json file

## Checks + features wishlist, and problems to think about:

* Improve efficiency by grouping all_frames by geo type and signal name instead of reading data in again via read_geo_signal_combo_files().
* Which, if any, specific geo_ids are missing (get list from historical data)
* Check for duplicate rows
* Check explicitly for large spikes (avg_val check can detect jumps in average value)
* Backfill problems, especially with JHU and USA Facts, where a change to old data results in a datapoint that doesn’t agree with surrounding data ([JHU examples](https://delphi-org.slack.com/archives/CF9G83ZJ9/p1600729151013900)) or is very different from the value it replaced. If date is already in the API, have any values changed significantly within the "backfill" window (use span_length setting). See [this](https://github.com/cmu-delphi/covidcast-indicators/pull/155#discussion_r504195207) for context.
* Use known erroneous/anomalous days of source data to tune static thresholds
* Different test thresholds for different files? Currently some control based on smoothed vs raw signals
* Data correctness and consistency over longer time periods (weeks to months). Compare data against long-ago (3 months?) API data for changes in trends.
  * Long-term trends and correlations between time series. Currently, checks only look at a data window of a few days
  * Any relevant anomaly detection packages already exist?
  * What sorts of hypothesis tests to use? See [time series trend analysis](https://www.genasis.cz/time-series/index.php?pg=home--trend-analysis).
  * See data-quality GitHub issues, Ryan’s [correlation notebook](https://github.com/cmu-delphi/covidcast/tree/main/R-notebooks), and Dmitry's [indicator validation notebook](https://github.com/cmu-delphi/covidcast-indicators/blob/deploy-jhu/testing_utils/indicator_validation.template.ipynb) for ideas
  * E.g. Doctor visits decreasing correlation with cases
  * E.g. WY/RI missing or very low compared to historical
* Use hypothesis testing p-values to decide when to raise error or not, instead of static thresholds. Many low but non-significant p-values will also raise error. See [here](https://delphi-org.slack.com/archives/CV1SYBC90/p1601307675021000?thread_ts=1600277030.103500&cid=CV1SYBC90) and [here](https://delphi-org.slack.com/archives/CV1SYBC90/p1600978037007500?thread_ts=1600277030.103500&cid=CV1SYBC90) for more background.
  * Order raised exceptions by p-value
  * Raise errors when one p-value (per geo region, e.g.) is significant OR when a bunch of p-values for that same type of test (differeng geo regions, e.g.) are "close" to significant
  * Correct p-values for multiple testing
  * Bonferroni would be easy but is sensitive to choice of "family" of tests; Benjamimi-Hochberg is a bit more involved but is less sensitive to choice of "family"
* Nicer formatting for error “report”
* Have separate error report sections for data validation checks (which are boolean) and statistical checks, where we want to present the most serious and significant issues first
* Statistical/anomaly checks should be included in the error report but should not block source data upload (i.e. not cause non-zero exit status)
* Run timing tests, check if saving intermediate files will improve effeciency
* Ensure validator runs on signals that require AWS credentials
* Improve efficiency and reduce load on API by pulling all checking_date ranges from API at once and subsetting in for loop
