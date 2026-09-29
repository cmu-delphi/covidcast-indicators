# NSSP ED respiratory data

We import the public NSSP Emergency Department Respiratory Daily data,
including the percentage of ED visits attributable to a given pathogen, from
CDC's Socrata portal. The data is provided at the state level and national
level with daily grain and is refreshed weekly.

NSSP ED respiratory source data:
https://data.cdc.gov/Public-Health-Surveillance/NSSP-Emergency-Department-Respiratory-Daily/vjzj-u7u8

## Geographical Levels
* `state`: reported from source using two-letter postal code
* `nation`: just `us` for now, reported from source
* `hhs`: not reported from source, so we compute it from state-level data
  using a population-weighted mean (same approach as the `nssp` indicator).
  States missing from the feed on a given day are extrapolated from the
  reporting states.

## Metrics
* `pct_ed_visits_ari`, `pct_ed_visits_covid`, `pct_ed_visits_influenza`,
  `pct_ed_visits_rsv`: percentage of emergency department patient visits for
  the specified pathogen, daily.
* `pct_ed_visits_ari_7dav`, `pct_ed_visits_covid_7dav`,
  `pct_ed_visits_influenza_7dav`, `pct_ed_visits_rsv_7dav`: trailing 7-day
  mean of the corresponding raw signal (a full 7-day window is required).

## Notes
* No Socrata app token is required; the feed is public. An optional token
  raises the API rate limit.
* The feed publishes percentages only, so `se` and `sample_size` are always
  missing (standard missing-value codes are applied).
* Rows with percentages outside [0, 100] are dropped with an error log.
* The feed is refreshed weekly and the most recent weeks are routinely
  revised by CDC; there is no provider-side version history.
