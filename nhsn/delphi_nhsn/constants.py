"""Registry for signal names."""
GEOS = ["state", "nation", "hhs"]

MAIN_DATASET_ID = "ua7e-t2fy"
PRELIMINARY_DATASET_ID = "mpgq-jmmr"

# column name from socrata
TOTAL_ADMISSION_COVID = "totalconfc19newadm"
TOTAL_ADMISSION_FLU = "totalconfflunewadm"
NUM_HOSP_REPORTINGS_FLU = "totalconfflunewadmhosprep"
NUM_HOSP_REPORTINGS_COVID = "totalconfc19newadmhosprep"

SIGNALS_MAP = {
    "confirmed_admissions_covid_ew": TOTAL_ADMISSION_COVID,
    "confirmed_admissions_flu_ew": TOTAL_ADMISSION_FLU,
    "confirmed_admissions_covid_prop_ew": [TOTAL_ADMISSION_COVID, NUM_HOSP_REPORTINGS_COVID],
    "confirmed_admissions_flu_prop_ew": [TOTAL_ADMISSION_FLU, NUM_HOSP_REPORTINGS_FLU],
}

TYPE_DICT = {
    "timestamp": "datetime64[ns]",
    "geo_id": str,
    "confirmed_admissions_covid_ew": float,
    "confirmed_admissions_flu_ew": float,
    "confirmed_admissions_covid_prop_ew": float,
    "confirmed_admissions_flu_prop_ew": float,
}

# signal mapping for secondary, preliminary source
PRELIM_SIGNALS_MAP = {
    "confirmed_admissions_covid_ew_prelim": TOTAL_ADMISSION_COVID,
    "confirmed_admissions_flu_ew_prelim": TOTAL_ADMISSION_FLU,
    "confirmed_admissions_covid_ew_prop_prelim": [TOTAL_ADMISSION_COVID, NUM_HOSP_REPORTINGS_COVID],
    "confirmed_admissions_flu_ew_prop_prelim": [TOTAL_ADMISSION_FLU, NUM_HOSP_REPORTINGS_FLU],
}
PRELIM_TYPE_DICT = {
    "timestamp": "datetime64[ns]",
    "geo_id": str,
    "confirmed_admissions_covid_ew_prelim": float,
    "confirmed_admissions_flu_ew_prelim": float,
    "confirmed_admissions_covid_ew_prop_prelim": float,
    "confirmed_admissions_flu_ew_prop_prelim": float,
}
