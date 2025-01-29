"""Registry for signal names."""

GEOS = ["state", "nation", "hhs"]

MAIN_DATASET_ID = "ua7e-t2fy"
PRELIM_DATASET_ID = "mpgq-jmmr"

# column name from socrata
TOTAL_ADMISSION_COVID_COL = "totalconfc19newadm"
TOTAL_ADMISSION_FLU_COL = "totalconfflunewadm"
NUM_HOSP_REPORTING_COVID_COL = "totalconfc19newadmhosprep"
NUM_HOSP_REPORTING_FLU_COL = "totalconfflunewadmhosprep"

# signal name
TOTAL_ADMISSION_COVID = "confirmed_admissions_covid_ew"
TOTAL_ADMISSION_FLU = "confirmed_admissions_flu_ew"
NUM_HOSP_REPORTING_COVID = "num_reporting_hospital_covid_ew"
NUM_HOSP_REPORTING_FLU = "num_reporting_hospital_flu_ew"

SIGNALS_MAP = {
    TOTAL_ADMISSION_COVID: TOTAL_ADMISSION_COVID_COL,
    TOTAL_ADMISSION_FLU: TOTAL_ADMISSION_FLU_COL,
    NUM_HOSP_REPORTING_COVID: NUM_HOSP_REPORTING_COVID_COL,
    NUM_HOSP_REPORTING_FLU: NUM_HOSP_REPORTING_FLU_COL,
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
    f"{TOTAL_ADMISSION_COVID}_prelim": TOTAL_ADMISSION_COVID_COL,
    f"{TOTAL_ADMISSION_FLU}_prelim": TOTAL_ADMISSION_FLU_COL,
    f"{NUM_HOSP_REPORTING_COVID}_prelim": NUM_HOSP_REPORTING_COVID_COL,
    f"{NUM_HOSP_REPORTING_FLU}_prelim": NUM_HOSP_REPORTING_FLU_COL,
}

PRELIM_TYPE_DICT = {
    "timestamp": "datetime64[ns]",
    "geo_id": str,
    f"{TOTAL_ADMISSION_COVID}_prelim": float,
    f"{TOTAL_ADMISSION_FLU}_prelim": float,
    f"{NUM_HOSP_REPORTING_COVID}_prelim": float,
    f"{NUM_HOSP_REPORTING_FLU}_prelim": float,
}
