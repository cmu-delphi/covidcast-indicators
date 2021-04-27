"""Registry for signals and geographies to process."""
from numpy import nan
from .generate_signals import sum_cols

NAN_VALUES = {
    None: nan,
    -999999: 1.5, # -999,999 represents the data range [0-3], so we use the range mean
    -999999.0: 1.5
}

CONFIRMED_ADMISSIONS = "confirmed_admissions_7d"
CONFIRMED_SUSPECTED_ADMISSIONS = "sum_confirmed_suspected_admissions_7d"

SIGNALS = [
    # (name, columns to use, operation, date offset)

    (CONFIRMED_ADMISSIONS,
     ["previous_day_admission_adult_covid_confirmed_7_day_sum",
      "previous_day_admission_pediatric_covid_confirmed_7_day_sum"],
     sum_cols,
     -1),

    (CONFIRMED_SUSPECTED_ADMISSIONS,
     ["previous_day_admission_adult_covid_confirmed_7_day_sum",
      "previous_day_admission_pediatric_covid_confirmed_7_day_sum",
      "previous_day_admission_adult_covid_suspected_7_day_sum",
      "previous_day_admission_pediatric_covid_suspected_7_day_sum"],
     sum_cols,
     -1),
]

GEO_RESOLUTIONS = [
    "county",
    "msa",
    "state",
    "hrr"
]
