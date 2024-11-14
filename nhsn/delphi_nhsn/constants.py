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

# column name from socrata
CONFIRMED_COVID_API = "total_admissions_all_covid_confirmed"
HOSPITAL_CONFIRMED_COVID_API = "percent_hospitals_admissions_all_covid_confirmed"

ADULT_CONFIRMED_COVID_API = "total_admissions_adult_covid_confirmed"
PEDIATRIC_CONFIRMED_COVID_API = "total_admissions_pediatric_covid_confirmed"

CONFIRMED_COVID_ADULT_PERCENT_API = "percent_adult_covid_admissions"
HOSPITAL_CONFIRMED_COVID_ADULT_PERCENT = "percent_hospitals_previous_day_admission_adult_covid_confirmed"

CONFIRMED_COVID_PEDIATRIC_PERCENT_API = "percent_pediatric_covid_admissions"
HOSPITAL_COVID_CONFIRMED_PEDIATRIC_PERCENT_API = "percent_hospitals_previous_day_admission_pediatric_covid_confirmed"

CONFIRMED_FLU_API = "total_admissions_all_influenza_confirmed"
HOSPITAL_CONFIRMED_FLU_API = "percent_hospitals_previous_day_admission_influenza_confirmed"


PARTIAL_SIGNALS = [
    CONFIRMED_COVID_API,
    ADULT_CONFIRMED_COVID_API,
    PEDIATRIC_CONFIRMED_COVID_API,
    CONFIRMED_COVID_ADULT_PERCENT_API,
    CONFIRMED_COVID_PEDIATRIC_PERCENT_API,
    CONFIRMED_FLU_API
]

SIGNALS_MAP = {
    "confirmed_admissions_covid": [CONFIRMED_COVID_API],
    "confirmed_admissions_flu": [CONFIRMED_FLU_API],
    "pct_confirmed_admissions_covid": [CONFIRMED_COVID_ADULT_PERCENT_API, CONFIRMED_COVID_PEDIATRIC_PERCENT_API]
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