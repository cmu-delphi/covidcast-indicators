"""Registry for signals and geographies to process."""
from .generate_signals import sum_cols

NAN_VALUE = -999999.0

ADMISSION_TOTAL = "total_admissions_7d"

SIGNALS = [
    # (name, columns to use, operation, date offset)
    (ADMISSION_TOTAL,
     ["previous_day_admission_adult_covid_confirmed_7_day_sum",
      "previous_day_admission_pediatric_covid_confirmed_7_day_sum"],
     sum_cols,
     -1),
]

GEO_RESOLUTIONS = [
    "county",
    "msa",
    "state",
    "hrr"
]
