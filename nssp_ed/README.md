# nssp_ed

COVIDcast indicator built on CDC's public **NSSP Emergency Department
Respiratory Daily** feed
(https://data.cdc.gov/Public-Health-Surveillance/NSSP-Emergency-Department-Respiratory-Daily/vjzj-u7u8),
from the National Syndromic Surveillance Program (NSSP). Each record is the
percentage of emergency department visits for a given pathogen out of all ED
visits, for one U.S. state (or the nation) on one day.

This indicator is the unauthenticated, public-data sibling of the existing
`nssp` indicator, which reads the restricted NSSP trajectories feed behind a
Socrata app token. Signal names are intentionally shared with `nssp`: in
COVIDcast, `source` + `signal` is the unique key, so `nssp-ed` signals sit
naturally alongside the `nssp` ones.

## Running the Indicator

The indicator is run by directly executing the Python module contained in this
directory. The safest way to do this is to create a virtual environment,
install the common DELPHI tools, and then install the module and its
dependencies. To do this, run the following command from this directory:

```
make install
```

No API key or other credential is needed: the feed is public. An optional
Socrata app token can be supplied as `indicator.socrata_token` in
`params.json` to raise the API rate limit.

All of the user-changable parameters are stored in `params.json`. To execute
the module and produce the output datasets (by default, in `receiving`), run
the following:

```
env/bin/python -m delphi_nssp_ed
```

To run the patcher (reconstruct a range of issue dates from raw backups):

```
env/bin/python -m delphi_nssp_ed.patch
```

with `params.json` containing `common.custom_run: true` and the `patch`
section described in `delphi_nssp_ed/patch.py`.

If you want to enter the virtual environment in your shell,
you can run `source env/bin/activate`. Run `deactivate` to leave the virtual environment.

Once you are finished, you can remove the virtual environment and
`params.json` by running `make clean` from this directory.

## Signals

Daily percent of ED visits for the pathogen (raw), plus a trailing 7-day mean
(`_7dav`):

* `pct_ed_visits_ari`, `pct_ed_visits_ari_7dav`
* `pct_ed_visits_covid`, `pct_ed_visits_covid_7dav`
* `pct_ed_visits_influenza`, `pct_ed_visits_influenza_7dav`
* `pct_ed_visits_rsv`, `pct_ed_visits_rsv_7dav`

## Geographical Levels

* `state`: reported from source using two-letter postal code (DC as `dc`)
* `nation`: reported from source as `us`
* `hhs`: population-weighted mean of the state percentages (same approach as
  the `nssp` indicator; states missing on a given day are extrapolated from
  the reporting states)

## Data-quality notes

* The feed is refreshed **weekly** (Wednesdays per CDC) with **daily**-grain
  records; expect `time_value` lag of roughly 7-14 days.
* Rows with `percent_visits` outside [0, 100] are implausible and are dropped
  with an error log (they do not fail the run).
* A schema change in the feed (missing columns, new pathogen labels) fails the
  pull loudly instead of emitting wrong data.
* The feed publishes percentages only: `se` and `sample_size` are always
  missing (standard missing-value codes applied).
* The feed is not versioned by CDC; the last few weeks of data are routinely
  revised. Patches reconstruct issues from raw backups and are approximations.

## Licensing

CDC-published U.S. federal government data (public domain). The Socrata
metadata for this dataset carries no explicit license field; U.S. federal
works are public domain under 17 U.S.C. section 105. Delphi team: please
confirm before production deployment.
