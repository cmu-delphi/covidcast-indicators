"""Registry for constants."""
# global constants
MIN_OBS = 50  # minimum number of observations in order to compute a proportion.
MAX_BORROW_OBS = 20 # maximum number of observations can be borrowed in geographical pooling
POOL_DAYS = 7  # number of days in the past (including today) to pool over
END_FROM_TODAY_MINUS = 5 # report data until - X days
EXPORT_DAY_RANGE = 40 # Number of dates to report
# Signal names
SENSORS = {
    "covid_ag_smoothed_pct_positive": (False, True),
    "covid_ag_raw_pct_positive": (False, False),
#    "covid_ag_smoothed_test_per_device": (True, True),
#    "covid_ag_raw_test_per_device": (True, False),
#    "flu_ag_smoothed_pct_positive": (False, True),
#    "flu_ag_raw_pct_positive": (False, False),
#    "flu_ag_smoothed_test_per_device": (True, True),
#    "flu_ag_raw_test_per_device": (True, False)
}
GEO_RESOLUTIONS = [
    "county",
    "msa",
    "hrr"
]
