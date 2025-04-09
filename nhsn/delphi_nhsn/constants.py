"""Registry for signal names."""

from datetime import timedelta

GEOS = ["state", "nation", "hhs"]

MAIN_DATASET_ID = "ua7e-t2fy"
PRELIM_DATASET_ID = "mpgq-jmmr"

# column name from socrata
TOTAL_ADMISSION_COVID_COL = "totalconfc19newadm"
TOTAL_ADMISSION_FLU_COL = "totalconfflunewadm"
TOTAL_ADMISSION_RSV_COL = "totalconfrsvnewadm"
NUM_HOSP_REPORTING_COVID_COL = "totalconfc19newadmhosprep"
NUM_HOSP_REPORTING_FLU_COL = "totalconfflunewadmhosprep"
NUM_HOSP_REPORTING_RSV_COL = "totalconfrsvnewadmhosprep"
# signal name
TOTAL_ADMISSION_COVID = "confirmed_admissions_covid_ew"
TOTAL_ADMISSION_FLU = "confirmed_admissions_flu_ew"
TOTAL_ADMISSION_RSV = "confirmed_admissions_rsv_ew"
NUM_HOSP_REPORTING_COVID = "hosprep_confirmed_admissions_covid_ew"
NUM_HOSP_REPORTING_FLU = "hosprep_confirmed_admissions_flu_ew"
NUM_HOSP_REPORTING_RSV = "hosprep_confirmed_admissions_rsv_ew"

SIGNALS_MAP = {
    TOTAL_ADMISSION_COVID: TOTAL_ADMISSION_COVID_COL,
    TOTAL_ADMISSION_FLU: TOTAL_ADMISSION_FLU_COL,
    TOTAL_ADMISSION_RSV: TOTAL_ADMISSION_RSV_COL,
    NUM_HOSP_REPORTING_COVID: NUM_HOSP_REPORTING_COVID_COL,
    NUM_HOSP_REPORTING_FLU: NUM_HOSP_REPORTING_FLU_COL,
    NUM_HOSP_REPORTING_RSV: NUM_HOSP_REPORTING_RSV_COL,
}

TYPE_DICT = {
    "timestamp": "datetime64[ns]",
    "geo_id": str,
    TOTAL_ADMISSION_COVID: float,
    TOTAL_ADMISSION_FLU: float,
    TOTAL_ADMISSION_RSV: float,
    NUM_HOSP_REPORTING_COVID: float,
    NUM_HOSP_REPORTING_FLU: float,
    NUM_HOSP_REPORTING_RSV: float,
}

# signal mapping for secondary, preliminary source
# made copy incase things would diverge

PRELIM_SIGNALS_MAP = {
    f"{TOTAL_ADMISSION_COVID}_prelim": TOTAL_ADMISSION_COVID_COL,
    f"{TOTAL_ADMISSION_FLU}_prelim": TOTAL_ADMISSION_FLU_COL,
    f"{TOTAL_ADMISSION_RSV}_prelim": TOTAL_ADMISSION_RSV_COL,
    f"{NUM_HOSP_REPORTING_COVID}_prelim": NUM_HOSP_REPORTING_COVID_COL,
    f"{NUM_HOSP_REPORTING_FLU}_prelim": NUM_HOSP_REPORTING_FLU_COL,
    f"{NUM_HOSP_REPORTING_RSV}_prelim": NUM_HOSP_REPORTING_RSV_COL,
}

PRELIM_TYPE_DICT = {
    "timestamp": "datetime64[ns]",
    "geo_id": str,
    f"{TOTAL_ADMISSION_COVID}_prelim": float,
    f"{TOTAL_ADMISSION_FLU}_prelim": float,
    f"{TOTAL_ADMISSION_RSV}_prelim": float,
    f"{NUM_HOSP_REPORTING_COVID}_prelim": float,
    f"{NUM_HOSP_REPORTING_FLU}_prelim": float,
    f"{NUM_HOSP_REPORTING_RSV}_prelim": float,
}

RECENTLY_UPDATED_DIFF = timedelta(days=1)
