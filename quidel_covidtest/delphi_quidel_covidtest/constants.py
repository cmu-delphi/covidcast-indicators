"""Registry for constants"""
# global constants
MIN_OBS = 50  # minimum number of observations in order to compute a proportion.
POOL_DAYS = 7  # number of days in the past (including today) to pool over
END_FROM_TODAY_MINUS = 5 # report data until - X days
EXPORT_DAY_RANGE = 40 # Number of dates to report
# Signal names
SMOOTHED_POSITIVE = "covid_ag_smoothed_pct_positive"
RAW_POSITIVE = "covid_ag_raw_pct_positive"
SMOOTHED_TEST_PER_DEVICE = "covid_ag_smoothed_test_per_device"
RAW_TEST_PER_DEVICE = "covid_ag_raw_test_per_device"
# Geo types
COUNTY = "county"
MSA = "msa"
HRR = "hrr"

GEO_RESOLUTIONS = [
    COUNTY,
    MSA,
    HRR
]
SENSORS = [
    SMOOTHED_POSITIVE,
    RAW_POSITIVE
#    SMOOTHED_TEST_PER_DEVICE,
#    RAW_TEST_PER_DEVICE
]
SMOOTHERS = {
    SMOOTHED_POSITIVE: (False, True),
    RAW_POSITIVE: (False, False)
#    SMOOTHED_TEST_PER_DEVICE: (True, True),
#    RAW_TEST_PER_DEVICE: (True, False)
}
