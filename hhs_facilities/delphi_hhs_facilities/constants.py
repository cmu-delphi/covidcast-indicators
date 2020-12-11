"""Registry for geographies to process."""

ADMISSION_TOTAL = "total_admission"

SIGNALS = [
    # (name, columns to use, operation, date offset)
    (ADMISSION_TOTAL,
     ["previous_day_admission_adult_covid_confirmed_7_day_sum",
      "previous_day_admission_pediatric_covid_confirmed_7_day_sum"],
     sum,
     -1),
]

GEO_RESOLUTIONS = [
    "county",
    "msa",
    "state",
    "hrr"
]
