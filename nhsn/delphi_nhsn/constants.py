"""Registry for signal names."""

GEOS = ["state", "nation", "hhs"]

MAIN_DATASET_ID = "ua7e-t2fy"
PRELIM_DATASET_ID = "mpgq-jmmr"

# column name from socrata
TOTAL_ADMISSION_COVID = "totalconfc19newadm"
TOTAL_ADMISSION_FLU = "totalconfflunewadm"
NUM_HOSP_REPORTING_COVID = "totalconfc19newadmhosprep"
NUM_HOSP_REPORTING_FLU = "totalconfflunewadmhosprep"

SIGNALS_MAP = {
    "confirmed_admissions_covid_ew": [TOTAL_ADMISSION_COVID],
    "confirmed_admissions_flu_ew": [TOTAL_ADMISSION_FLU],
    "num_reporting_hospital_covid_ew": [NUM_HOSP_REPORTING_COVID],
    "num_reporting_hospital_flu_ew": [NUM_HOSP_REPORTING_FLU],
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
    "num_reporting_hospital_covid_ew_prelim": [NUM_HOSP_REPORTING_COVID],
    "num_reporting_hospital_flu_ew_prelim": [NUM_HOSP_REPORTING_FLU],
}

PRELIM_TYPE_DICT = {
    "timestamp": "datetime64[ns]",
    "geo_id": str,
    TOTAL_ADMISSION_COVID: float,
    TOTAL_ADMISSION_FLU: float,
    NUM_HOSP_REPORTING_COVID: float,
    NUM_HOSP_REPORTING_FLU: float,
}
