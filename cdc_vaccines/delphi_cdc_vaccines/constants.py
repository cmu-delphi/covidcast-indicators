"""Registry for variations."""


from delphi_utils import Smoother

FULL_VAC_SUM     = "cumulative_counts_tot_vaccine"
FULL_VAC         = "counts_tot_vaccine"
FULL_VAC_SUM_12P = "cumulative_counts_tot_vaccine_12P"
FULL_VAC_12P     = "counts_tot_vaccine_12P"
FULL_VAC_SUM_18P = "cumulative_counts_tot_vaccine_18P"
FULL_VAC_18P     = "counts_tot_vaccine_18P"
FULL_VAC_SUM_65P = "cumulative_counts_tot_vaccine_65P"
FULL_VAC_65P     = "counts_tot_vaccine_65P"
PART_VAC_SUM     = "cumulative_counts_part_vaccine"
PART_VAC         = "counts_part_vaccine"
PART_VAC_SUM_12P = "cumulative_counts_part_vaccine_12P"
PART_VAC_12P     = "counts_part_vaccine_12P"
PART_VAC_SUM_18P = "cumulative_counts_part_vaccine_18P"
PART_VAC_18P     = "counts_part_vaccine_18P"
PART_VAC_SUM_65P = "cumulative_counts_part_vaccine_65P"
PART_VAC_65P     = "counts_part_vaccine_65P"



SIGNALS = [
	FULL_VAC_SUM     ,
	FULL_VAC         ,
	FULL_VAC_SUM_12P ,
	FULL_VAC_12P     ,
	FULL_VAC_SUM_18P ,
	FULL_VAC_18P     ,
	FULL_VAC_SUM_65P ,
	FULL_VAC_65P     ,
	PART_VAC_SUM     ,
	PART_VAC         ,
	PART_VAC_SUM_12P ,
	PART_VAC_12P     ,
	PART_VAC_SUM_18P ,
	PART_VAC_18P     ,
	PART_VAC_SUM_65P ,
	PART_VAC_65P
]

GEOS = [
    "nation",
    "state_code",
    "hrr",
    "hhs",
    "msa"
]

SMOOTHERS = [
    (Smoother("identity", impute_method=None), ""),
    (Smoother("moving_average", window_length=7), "_7dav"),
]
