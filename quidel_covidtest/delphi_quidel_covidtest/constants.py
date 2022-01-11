"""Registry for constants."""
# global constants
MIN_OBS = 50  # minimum number of observations in order to compute a proportion.
POOL_DAYS = 7  # number of days in the past (including today) to pool over
END_FROM_TODAY_MINUS = 5 # report data until - X days
# Signal names
SMOOTHED_POSITIVE = "covid_ag_smoothed_pct_positive"
RAW_POSITIVE = "covid_ag_raw_pct_positive"
SMOOTHED_TEST_PER_DEVICE = "covid_ag_smoothed_test_per_device"
RAW_TEST_PER_DEVICE = "covid_ag_raw_test_per_device"
# Geo types
COUNTY = "county"
MSA = "msa"
HRR = "hrr"
HHS = "hhs"
NATION = "nation"
STATE = "state"

PARENT_GEO_RESOLUTIONS = [
    COUNTY,
    MSA,
    HRR,
]

NONPARENT_GEO_RESOLUTIONS = [
    HHS,
    NATION,
    STATE
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
AGE_GROUPS = [
    "total",
    "age_0to5", 
    "age_5to13", 
    "age_14to17", 
    "age_18to49", 
    "age_50to64", 
    "age_65to74", 
    "age_75toOlder"
]
