# Validator checks and features

## Current checks for indicator source data

* Missing dates within the selected range
* Recognized file name format
* Recognized geographical type (county, state, etc)
* Recognized geo id format (e.g. state is two lowercase letters)
* Specific geo id has been seen before, in historical data
* Missing geo type + signal + date combos based on the geo type + signal combos Covidcast metadata says should be available
* Missing ‘val’ values
* Negative ‘val’ values
* Out-of-range ‘val’ values (>0 for all signals, <=100 for percents, <=100 000 for proportions)
* Missing ‘se’ values
* Stderr != 0
* If signal and stderr both = 0 (seen in Quidel data due to lack of Jeffreys correction, [issue 255](https://github.com/cmu-delphi/covidcast-indicators/issues/255#issuecomment-692196541))
* Missing ‘sample_size’ values
* Appropriate ‘sample_size’ values, ≥ 100 (default) or user-defined threshold
* Most recent date seen in source data is recent enough, < 1 day ago (default) or user-defined on a per-signal basis
* Most recent date seen in source data is not in the future
* Most recent date seen in source data is not older than most recent date seen in reference data
* Similar number of obs per day as recent API data (static threshold)
* Similar average value as API data (static threshold)
* Outliers in cases and deaths signals using [this method](https://github.com/cmu-delphi/covidcast-forecast/tree/dev/corrections/data_corrections)
* Source data for specified date range is empty
* API data for specified date range is empty
* Duplicate rows


## Current features

* Errors and warnings are summarized in class attribute and stored in log files (file path to be specified in params)
* If any non-suppressed errors are raised and dry-run is set to False, the validation process exits with non-zero status
* Various check settings are controllable via indicator-specific params.json files
* User can manually disable specific checks for specific datasets using a field in the params.json file
* User can enable test mode (checks only a small number of data files) using a field in the params.json file
* User can enable dry-run mode (prevents system exit with error and ensures that success() method returns True) using a field in the params.json file


## Checks + features wishlist, and problems to think about

### Starter/small issues

* Backfill problems, especially with JHU and USA Facts, where a change to old data results in a datapoint that doesn’t agree with surrounding data ([JHU examples](https://delphi-org.slack.com/archives/CF9G83ZJ9/p1600729151013900)) or is very different from the value it replaced. If date is already in the API, have any values changed significantly within the "backfill" window (use span_length setting). See [this](https://github.com/cmu-delphi/covidcast-indicators/pull/155#discussion_r504195207) for context.
* Run check_missing_date_files (or similar) on every geo type-signal type separately in comparative checks loop.

### Larger issues

* Expand framework to support nchs_mortality, which is provided on a weekly basis and has some differences from the daily data. E.g. filenames use a different format ("weekly_YYYYWW_geotype_signalname.csv")
* Make backtesting framework so new checks can be run individually on historical indicator data to tune false positives, output verbosity, understand frequency of error raising, etc. Should pull data from API the first time and save locally in `cache` dir.
* Add DETAILS.md doc with detailed descriptions of what each check does and how. Will be especially important for statistical/anomaly detection checks.
* Easier-to-read error report
  * Potentially set `__print__()` method in ValidationError class
  * E.g. if a single type of error is raised for many different datasets, summarize all error messages into a single message? But it still has to be clear how to suppress each individually
  * Consider adding summary counts of each type of error, rather than just a combined number
* Check for data sources that wrongly report all zeroes
  * E.g. the error with the Wisconsin data for the 10/26/2020 forecasts
  * Wary of a purely static check for this
  * Regions with small populations (e.g. small counties or MSAs) and rare signals (e.g. deaths, since it's << cases) likely to cause false positives
  * This test is captured by `check_avg_val_vs_reference`, as long as erroneous zeroes occur for less than the reference period (1-2 weeks)
  * Also partially captured by `check_positive_negative_spikes`, depending on `size_cut` setting. However, `check_positive_negative_spikes` has limited applicability and only applies to incident cases and deaths signals.
* Instead of failing validation for a single check error, compare rate of check failures to historical rate? Requires caching and updating historical failure rates by signal, data source, and geo region. Unclear if worthwhile.
* Improve performance and reduce runtime (no particular goal, just handle low-hanging fruit and avoid being painfully slow!)
  * Profiling (iterate)
  * Save intermediate files?
  * Currently a bottleneck at "individual file checks" section. Parallelize?
  * Make `all_frames` MultiIndex-ed by geo type and signal name? Make a dict of data indexed by geo type and signal name? May improve performance or may just make access more readable.
* Revisit tuning of thresholds for outlier-related checks (`check_positive_negative_spikes`, `check_avg_val_vs_reference`) or parameters set in params.json.template
  * Currently using manually tuned z-score thresholds using 1-2 months of data (June-July 2021), but signal behavior may change
  * Certain signals (e.g. locally monotonic signals, sparse signals) exhibit different behavior and may require signal-specific paramters for checks such as z-scores.
  * Use caching to store params and update these dynamically using recent data?
* Create different error levels for checks beyond warning and critical: useful because certain checks clearly indicate some form of data corruption (e.g. `check_missing_date_files` identifying missing data), while other checks just report abnormal behavior that may be able to be explained.
* Compare current validator model against known instances of data issues to evaluate performance (may be difficult if data corrections are issued)

### Longer-term features

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
  * Use prophet package? Would require 2-3 months of API data.