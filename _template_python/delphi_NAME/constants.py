"""Registry for variations

Lists of geos to produce, signals to produce, how our signal names map to
provided field names, dataset ids, data source API base URL, etc.
"""

from datetime import timedelta

from delphi_utils import Smoother

# Fixed URL where data is located
SOURCE_URL = "data.cdc.gov"
# Fixed ID of dataset within data source system
TABLE_ID = "g653-rqe2"

# Map between signal names as provided by the data source and the signal names
# we want to output
SIGNALS_MAP = {
	# provided_field_name: delphi_output_signal_name
    "percent_visits_covid": "pct_ed_visits_covid",
    "percent_visits_influenza": "pct_ed_visits_influenza",
    "percent_visits_rsv": "pct_ed_visits_rsv",
    "percent_visits_combined": "pct_ed_visits_combined",
    ## If smoothed data is provided by the source, provide name map here
    ## rather than making the SMOOTHERS_MAP below
    # "percent_visits_smoothed_covid": "smoothed_pct_ed_visits_covid",
    # "percent_visits_smoothed_influenza": "smoothed_pct_ed_visits_influenza",
    # "percent_visits_smoothed_rsv": "smoothed_pct_ed_visits_rsv",
    # "percent_visits_smoothed": "smoothed_pct_ed_visits_combined",
}
# Data provided in a different format may not let us map directly between old
# and new signal names, so consider this format as well:
#   FULL_TIME = "full_time_work_prop"
#   PART_TIME = "part_time_work_prop"
#   COVIDNET = "covidnet"
#
#   SIGNALS = [
#       FULL_TIME,
#       PART_TIME,
#       COVIDNET
#   ]

# Delphi output signal names
SIGNALS = [val for (key, val) in SIGNALS_MAP.items()]

GEO_RESOLUTIONS = [
    "county",
    "msa",
    "hrr",
    "state",
    "hhs",
    "nation"
]

SMOOTHERS = ["raw", "smoothed"]
SMOOTHERS_MAP = {
	# For each type of smoothed output, provide
	#   - signal name suffix
	#   - smoother function
	#   - (optional) lambda to set min export date for
	#     delphi_utils.create_export_csv relative to export_start_date defined
	#     in params.json. export_start_date is usually the date that the
	#     relevant history of the data source starts, NOT the first date that
	#     the current run should output
    "raw":               ("",
    					  Smoother("identity", impute_method=None),
                          lambda d: d - timedelta(days=7)),
    "smoothed":          ("_7dav"
    					  Smoother("moving_average", window_length=7,
                                   impute_method='zeros'),
						  lambda d: d)
}

NEWLINE = "\n"
