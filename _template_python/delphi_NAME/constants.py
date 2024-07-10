"""Registry for variations

Lists of geos to produce, signals to produce, dataset ids, data source URL, etc.
"""

## example:
#GEOS = [
#    "nation",
#    "hhs",
#    "state",
#    "hrr",
#    "msa",
#    "county"
#]

GEOS = []

## example:
# FULL_TIME = "full_time_work_prop"
# PART_TIME = "part_time_work_prop"
# COVIDNET = "covidnet"
#
# SIGNALS = [
#     FULL_TIME,
#     PART_TIME,
#     COVIDNET
# ]

SIGNALS = []

## example:
#SMOOTHERS = [
#    (Smoother("identity", impute_method=None), ""),
#    (Smoother("moving_average", window_length=7), "_7dav"),
#]

SMOOTHERS = []
