"""
Registry for variations
"""

GEOS = [
   "msa",
   "hrr",
   "nation",
   "state"
]

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

SIGNALS = [
	"confirmed_cases",
	"confirmed_deaths"
]

SENSOR_NAME_MAP = {
    "new_counts":           ("incidence_num", False),
    "cumulative_counts":    ("cumulative_num", False),
    "incidence":            ("incidence_prop", False),
    "cumulative_prop":      ("cumulative_prop", False),
}

SMOOTHERS = [
   (Smoother("identity", impute_method=None), ""),
   (Smoother("moving_average", window_length=7), "_7dav"),
]

# Columns to drop the the data frame.
DROP_COLUMNS = [
    "countyfips",
    "county name",
    "state",
    "statefips"
]

BASE_URL = "https://data.cdc.gov/resource/9mfq-cb36.json?$limit=50000"
NEXT_PAGE_FILTER = "&$offset={}"
DATE_FILTER = "&$where=((created_at between '{}' and '{}') AND (submission_date between '{}' and '{}'))"
