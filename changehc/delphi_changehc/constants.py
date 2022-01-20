"""Registry for signal names and geo types."""
SMOOTHED = "smoothed_outpatient_covid"
SMOOTHED_ADJ = "smoothed_adj_outpatient_covid"
SMOOTHED_CLI = "smoothed_outpatient_cli"
SMOOTHED_ADJ_CLI = "smoothed_adj_outpatient_cli"
SMOOTHED_FLU = "smoothed_outpatient_flu"
SMOOTHED_ADJ_FLU = "smoothed_adj_outpatient_flu"
SIGNALS = [SMOOTHED, SMOOTHED_ADJ, SMOOTHED_CLI, SMOOTHED_ADJ_CLI, SMOOTHED_FLU, SMOOTHED_ADJ_FLU]
NA = "NA"
HRR = "hrr"
FIPS = "fips"

EXPECTED_FILES_PER_DROP = 7
