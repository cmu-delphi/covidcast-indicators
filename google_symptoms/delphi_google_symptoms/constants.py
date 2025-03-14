"""Registry for constants."""

from datetime import datetime, timedelta

from delphi_utils import Smoother

# global constants

SYMPTOM_SETS = {
    "s01": ["Cough", "Phlegm", "Sputum", "Upper respiratory tract infection"],
    "s02": ["Nasal congestion", "Post nasal drip", "Rhinorrhea", "Sinusitis",
            "Rhinitis", "Common cold"],
    "s03": ["Fever", "Hyperthermia", "Chills", "Shivering", "Low grade fever"],
    "s04": ["Shortness of breath", "Wheeze", "Croup", "Pneumonia", "Asthma",
            "Crackles", "Acute bronchitis", "Bronchitis"],
    "s05": ["Anosmia", "Dysgeusia", "Ageusia"],
    "s06": ["Laryngitis", "Sore throat", "Throat irritation"],
    "s07": ["Conjunctivitis", "Red eye", "Epiphora", "Eye pain", "Rheum"],
    "scontrol": ["Type 2 diabetes", "Urinary tract infection", "Hair loss",
                 "Candidiasis", "Weight gain"]
}

COMBINED_METRIC = list(SYMPTOM_SETS.keys())

METRICS = list()
for combmetric in COMBINED_METRIC:
    METRICS = METRICS + SYMPTOM_SETS[combmetric]

SMOOTHERS = ["raw", "smoothed"]

GEO_RESOLUTIONS = {
    "state": "state",
    "county": "county",
    "msa": "county",
    "hrr": "county",
    "hhs": "state",
    "nation": "state",
}

SMOOTHERS_MAP = {
    "raw":               (Smoother("identity", impute_method=None),
                          lambda d: d - timedelta(days=7)),
    "smoothed":          (Smoother("moving_average", window_length=7,
                                   impute_method='zeros'), lambda d: d)
}

DTYPE_CONVERSIONS = {'open_covid_region_code':'string','date': 'datetime64'}
for index, symptom in enumerate(METRICS):
    key = "symptom_" + METRICS[index].replace(" ","_")
    DTYPE_CONVERSIONS[key] = "float"

STATE_TO_ABBREV = {'Alabama': 'al',
                   'Alaska': 'ak',
                   #                   'American Samoa': 'as',
                   'Arizona': 'az',
                   'Arkansas': 'ar',
                   'California': 'ca',
                   'Colorado': 'co',
                   'Connecticut': 'ct',
                   'Delaware': 'de',
                   #                   'District of Columbia': 'dc',
                   'Florida': 'fl',
                   'Georgia': 'ga',
                   #                   'Guam': 'gu',
                   'Hawaii': 'hi',
                   'Idaho': 'id',
                   'Illinois': 'il',
                   'Indiana': 'in',
                   'Iowa': 'ia',
                   'Kansas': 'ks',
                   'Kentucky': 'ky',
                   'Louisiana': 'la',
                   'Maine': 'me',
                   'Maryland': 'md',
                   'Massachusetts': 'ma',
                   'Michigan': 'mi',
                   'Minnesota': 'mn',
                   'Mississippi': 'ms',
                   'Missouri': 'mo',
                   'Montana': 'mt',
                   'Nebraska': 'ne',
                   'Nevada': 'nv',
                   'New_Hampshire': 'nh',
                   'New_Jersey': 'nj',
                   'New_Mexico': 'nm',
                   'New_York': 'ny',
                   'North_Carolina': 'nc',
                   'North_Dakota': 'nd',
                   #                   'Northern Mariana Islands': 'mp',
                   'Ohio': 'oh',
                   'Oklahoma': 'ok',
                   'Oregon': 'or',
                   'Pennsylvania': 'pa',
                   #                   'Puerto Rico': 'pr',
                   'Rhode_Island': 'ri',
                   'South_Carolina': 'sc',
                   'South_Dakota': 'sd',
                   'Tennessee': 'tn',
                   'Texas': 'tx',
                   'Utah': 'ut',
                   'Vermont': 'vt',
                   #                   'Virgin Islands': 'vi',
                   'Virginia': 'va',
                   'Washington': 'wa',
                   'West_Virginia': 'wv',
                   'Wisconsin': 'wi',
                   'Wyoming': 'wy'}

DC_FIPS = "11001"

FULL_BKFILL_START_DATE = datetime.strptime("2020-02-20", "%Y-%m-%d")

PAD_DAYS = 7
