"""
Registry for variations
"""

GEOS = [
    "nation",
    "state",
    # "hrr",
    # "msa",
    # "hhs",
    # "county",
    # "wwss",  # wastewater sample site, name will probably need to change
]

## example:
#
# FULL_TIME = "full_time_work_prop"
# PART_TIME = "part_time_work_prop"
# COVIDNET = "covidnet"
#
# SIGNALS = [
#     FULL_TIME,
#     PART_TIME,
#     COVIDNET
# ]

SIGNALS = ["pcr_conc_smoothed"]
METRIC_SIGNALS = ["detect_prop_15d", "percentile", "ptc_15d"]
METRIC_DATES = ["issue", "date_start", "date_end"]
SAMPLE_SITE_NAMES = {
    "wwtp_jurisdiction": "category",
    "wwtp_id": int,
    "wwtp_id": int,
    "reporting_jurisdiction": "category",
    "sample_location": "category",
    "county_names": "category",
    "county_fips": "category",
    "population_served": float,
    "sampling_prior": bool,
    "sample_location_specify": float,
}
SIG_DIGITS = 7

## example:
# SMOOTHERS = [
#    (Smoother("identity", impute_method=None), ""),
#    (Smoother("moving_average", window_length=7), "_7dav"),
# ]

SMOOTHERS = []
