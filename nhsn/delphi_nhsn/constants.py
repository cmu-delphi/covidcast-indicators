"""Registry for signal names."""

GEOS = ["state", "nation", "hhs"]

# column name from socrata
TOTAL_ADMISSION_COVID = "totalconfc19newadm"
TOTAL_ADMISSION_FLU = "totalconfflunewadm"
NUM_HOSP_REPORTING_COVID = "totalconfc19newadmhosprep"
NUM_HOSP_REPORTING_FLU = "totalconfflunewadmhosprep"

SIGNALS_MAP = {
    "confirmed_admissions_covid_ew": [TOTAL_ADMISSION_COVID],
    "confirmed_admissions_flu_ew": [TOTAL_ADMISSION_FLU],
    "confirmed_admissions_covid_prop_ew": [TOTAL_ADMISSION_COVID, NUM_HOSP_REPORTING_COVID],
    "confirmed_admissions_flu_prop_ew": [TOTAL_ADMISSION_FLU, NUM_HOSP_REPORTING_FLU]
}

TYPE_DICT = {
    "timestamp": "datetime64[ns]",
    "geo_id": str,
    TOTAL_ADMISSION_COVID: float,
    TOTAL_ADMISSION_FLU: float,
    NUM_HOSP_REPORTING_COVID: float,
    NUM_HOSP_REPORTING_FLU: float,
}

# signal mapping for secondary, preliminary source
# made copy incase things would diverge

PRELIM_SIGNALS_MAP = {
    "confirmed_admissions_covid_ew_prelim": [TOTAL_ADMISSION_COVID],
    "confirmed_admissions_flu_ew_prelim": [TOTAL_ADMISSION_FLU],
    "confirmed_admissions_covid_prop_ew_prelim": [TOTAL_ADMISSION_COVID, NUM_HOSP_REPORTING_COVID],
    "confirmed_admissions_flu_prop_ew_prelim": [TOTAL_ADMISSION_FLU, NUM_HOSP_REPORTING_FLU]
}

PRELIM_TYPE_DICT = {
    "timestamp": "datetime64[ns]",
    "geo_id": str,
    TOTAL_ADMISSION_COVID: float,
    TOTAL_ADMISSION_FLU: float,
    NUM_HOSP_REPORTING_COVID: float,
    NUM_HOSP_REPORTING_FLU: float,
}
