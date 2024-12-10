"""Registry for signal names."""

GEOS = ["state", "nation", "hhs"]

# column name from socrata
TOTAL_ADMISSION_COVID_API = "totalconfc19newadm"
TOTAL_ADMISSION_FLU_API = "totalconfflunewadm"

SIGNALS_MAP = {
    "confirmed_admissions_covid_ew": TOTAL_ADMISSION_COVID_API,
    "confirmed_admissions_flu_ew": TOTAL_ADMISSION_FLU_API,
}

TYPE_DICT = {
    "timestamp": "datetime64[ns]",
    "geo_id": str,
    "confirmed_admissions_covid": float,
    "confirmed_admissions_flu": float,
}

# signal mapping for secondary, preliminary source
PRELIM_SIGNALS_MAP = {
    "confirmed_admissions_covid_ew_prelim": TOTAL_ADMISSION_COVID_API,
    "confirmed_admissions_flu_ew_prelim": TOTAL_ADMISSION_FLU_API,
}
PRELIM_TYPE_DICT = {
    "timestamp": "datetime64[ns]",
    "geo_id": str,
    "prelim_confirmed_admissions_covid": float,
    "prelim_confirmed_admissions_flu": float,
}
