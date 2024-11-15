"""Registry for signal names."""
from delphi_utils import Smoother

GEOS = [
    "nation",
    "hhs",
    "state"
]
# FROM HHS
CONFIRMED = "confirmed_admissions_covid_1d"
SUM_CONF_SUSP = "sum_confirmed_suspected_admissions_covid_1d"
CONFIRMED_PROP = "confirmed_admissions_covid_1d_prop"
SUM_CONF_SUSP_PROP = "sum_confirmed_suspected_admissions_covid_1d_prop"
CONFIRMED_FLU = "confirmed_admissions_influenza_1d"
CONFIRMED_FLU_PROP = CONFIRMED_FLU+"_prop"

# FROM CDC/METADATA
CONFIRMED_COVID = "Weekly Total COVID-19 Admissions"
HOSPITAL_CONFIRMED_COVID = "Percent Hospitals Reporting Total COVID-19 Admissions"

ADULT_CONFIRMED_COVID = "Weekly Total Adult COVID-19 Admissions"
PEDIATRIC_CONFIRMED_COVID = "Weekly Total Pediatric COVID-19 Admissions"

CONFIRMED_COVID_ADULT_PERCENT = "Percent Adult COVID-19 Admissions"
HOSPITAL_CONFIRMED_COVID_ADULT_PERCENT = "Percent Hospitals Reporting Adult COVID-19 Admissions"

CONFIRMED_COVID_PEDIATRIC_PERCENT = "Percent Pediatric COVID-19 Admissions"
HOSPITAL_COVID_CONFIRMED_PEDIATRIC_PERCENT = "Percent Hospitals Reporting Pediatric COVID-19 Admissions"

CONFIRMED_FLU = "Weekly Total Influenza Admissions"
HOSPITAL_CONFIRMED_FLU = "Percent Hospitals Reporting Influenza Admissions"

CONFIRMED_FLU_ADULT_PERCENT = "Percent Adult Influenza Admissions"
HOSPITAL_CONFIRMED_FLU_ADULT_PERCENT = "Percent Hospitals Reporting TotalPatients Hospitalized with Influenza"

CONFIRMED_FLU_PEDIATRIC_PERCENT = "Percent Pediatric Influenza Admissions"
HOSPTIAL_CONFIRMED_FLU_PEDIATRIC_PERCENT = "Percent Hospitals Reporting Pediatric Influenza Admissions"

AGE_GROUPS = [
    "0to4",
    "5to17",
    "18to49",
    "50to64",
    "65to74",
    "75plus",
    "unk"
]

# column name from socrata
TOTAL_ADMISSION_COVID_API = "totalconfc19newadm"
ADULT_ADMISSION_COVID_API = "numconfc19newadmadult"
TOTAL_ADULT_ADMISSION_COVID_API = "totalconfc19newadmadult"
PEDIATRIC_ADMISSION_COVID_API = "numconfc19newadmped"
TOTAL_PEDIATRIC_ADMISSION_COVID_API = "totalconfc19newadmped"

PERCENT_ADULT_ADMISSION_COVID_API = "pctconfrsvnewadmadult"
PERCENT_PEDIATRIC_ADMISSION_COVID_API = "pctconfrsvnewadmped"

TOTAL_ADMISSION_FLU_API = "totalconfflunewadm"
ADULT_ADMISSION_FLU_API = "numconffluhosppatsadult"
TOTAL_ADULT_ADMISSION_FLU_API = "numconfflunewadmadult"
PEDIATRIC_ADMISSION_FLU_API = "numconfflunewadmped"
TOTAL_PEDIATRIC_ADMISSION_FLU_API = "totalconfflunewadmped"

TOTAL_ADMISSION_RSV_API = "totalconfrsvnewadm"
ADULT_ADMISSION_RSV_API = "numconfrsvnewadmadult"
TOTAL_ADULT_ADMISSION_RSV_API = "totalconfrsvnewadmadult"
PEDIATRIC_ADMISSION_RSV_API = "numconfrsvnewadmped"
TOTAL_PEDIATRIC_ADMISSION_RSV_API = "totalconfrsvnewadmped"

PARTIAL_SIGNALS = [
    TOTAL_ADMISSION_COVID_API,
    ADULT_ADMISSION_COVID_API,
    TOTAL_ADULT_ADMISSION_COVID_API,
    PEDIATRIC_ADMISSION_COVID_API,
    TOTAL_PEDIATRIC_ADMISSION_COVID_API,
    PERCENT_ADULT_ADMISSION_COVID_API,
    PERCENT_PEDIATRIC_ADMISSION_COVID_API,
    TOTAL_ADMISSION_FLU_API,
    ADULT_ADMISSION_FLU_API,
    TOTAL_ADULT_ADMISSION_FLU_API,
    PEDIATRIC_ADMISSION_FLU_API,
    TOTAL_PEDIATRIC_ADMISSION_FLU_API,
    TOTAL_ADMISSION_RSV_API,
    ADULT_ADMISSION_RSV_API,
    TOTAL_ADULT_ADMISSION_RSV_API,
    PEDIATRIC_ADMISSION_RSV_API,
    TOTAL_PEDIATRIC_ADMISSION_RSV_API,
]

SIGNALS_MAP = {
    "confirmed_admissions_covid": [TOTAL_ADMISSION_COVID_API],
    "confirmed_admissions_flu": [TOTAL_ADMISSION_FLU_API],
    "confirmed_admissions_rsv": [TOTAL_ADMISSION_RSV_API],
}

TYPE_DICT = {
        "timestamp": "datetime64[ns]",
        "jurisdiction": str,
    }

TYPE_DICT.update({key: float for key in PARTIAL_SIGNALS})



SMOOTHERS = [
    (Smoother("identity", impute_method=None), ""),
    (Smoother("moving_average", window_length=7), "_7dav"),
]