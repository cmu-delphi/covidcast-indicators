"""Registry for constants."""
# global constants
METRICS = [
        "covid_deaths", "total_deaths", "percent_of_expected_deaths",
        "pneumonia_deaths", "pneumonia_and_covid_deaths", "influenza_deaths",
        "pneumonia_influenza_or_covid_19_deaths"
]
SENSOR_NAME_MAP = {
        "covid_deaths": "deaths_covid_incidence",
        "total_deaths": "deaths_allcause_incidence",
        "percent_of_expected_deaths": "deaths_percent_of_expected",
        "pneumonia_deaths": "deaths_pneumonia_notflu_incidence",
        "pneumonia_and_covid_deaths": "deaths_covid_and_pneumonia_notflu_incidence",
        "influenza_deaths": "deaths_flu_incidence",
        "pneumonia_influenza_or_covid_19_deaths": "deaths_pneumonia_or_flu_or_covid_incidence"
}
SENSORS = [
        "num",
        "prop"
]
INCIDENCE_BASE = 100000
GEO_RES = "state"
