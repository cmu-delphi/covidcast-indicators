"""Registry for constants."""
from functools import partial
from datetime import timedelta

from delphi_utils import Smoother

from .smooth import (
    identity_OLD,
    kday_moving_average_OLD
)

# global constants
METRICS = ["Abdominal pain",
           "Anosmia",
           "Ageusia",
           "Acute bronchitis",
           "Asthma",
           "Bronchitis",
           "Chest pain",
           "Chills",
           "Cluster headache",
           "Common cold",
           "Cough",
           "Crackles",
           "Croup",
           "Diarrhea",
           "Dizziness",
           "Dysgeusia",
           "Fatigue",
           "Fever",
           "Headache",
           "Hyperthermia",
           "Indigestion",
           "Laryngitis",
           "Lightheadedness",
           "Low grade fever",
           "Migraine",
           "Muscle weakness",
           "Myalgia",
           "Nausea",
           "Nasal congestion",
           "Night sweats",
           "Pain",
           "Perspiration",
           "Phlegm",
           "Pneumonia",
           "Post nasal drip",
           "Rheum",
           "Rhinitis",
           "Rhinorrhea",
           "Sharp pain",
           "Shivering",
           "Shortness of breath",
           "Sinusitis",
           "Sore throat",
           "Sputum",
           "Throat irritation",
           "Upper respiratory tract infection",
           "Vomiting",
           "Weakness",
           "Wheeze",
           "hyperhidrosis",
           "Type 2 diabetes",
           "Urinary tract infection",
           "Hair loss",
           "Candidiasis",
           "Weight gain"]

COMBINED_METRIC = ["sum_anosmia_ageusia", "S01", "S02", "S03",
                   #"S04",
                   "S05", "S06",
                   #"S07",
                   "S08",
                   #"S09", "S10",
                   "SControl"]

SYMPTOM_SETS = {
    "sum_anosmia_ageusia": ["Anosmia", "Ageusia"],
    "S01": ["Cough", "Phlegm", "Sputum", "Upper respiratory tract infection"],
    "S02": ["Nasal congestion", "Post nasal drip", "Rhinorrhea", "Sinusitis", "Rhinitis", "Common cold"],
    "S03": ["Fever", "Hyperthermia", "Chills", "Shivering", "Low grade fever"],
    #"S04": ["Fatigue", "Weakness", "Muscle weakness", "Myalgia", "Pain"],
    "S05": ["Shortness of breath", "Wheeze", "Croup", "Pneumonia", "Asthma", "Crackles", "Acute bronchitis", "Bronchitis"],
    "S06": ["Anosmia", "Dysgeusia", "Ageusia"],
    #"S07": ["Nausea", "Vomiting", "Diarrhea", "Indigestion", "Abdominal pain"],
    "S08": ["Laryngitis", "Sore throat", "Throat irritation"],
    #"S09": ["Headache", "Migraine", "Cluster headache", "Dizziness", "Lightheadedness"],
    #"S10": ["Night sweats","Perspiration", "hyperhidrosis"],
    "SControl": ["Type 2 diabetes", "Urinary tract infection", "Hair loss", "Candidiasis", "Weight gain"]
}


SMOOTHERS = ["raw_OLD", "smoothed_OLD", "raw", "smoothed"]
GEO_RESOLUTIONS = [
    "state",
    "county",
    "msa",
    "hrr",
    "hhs",
    "nation"
]

seven_day_moving_average = partial(kday_moving_average_OLD, k=7)
SMOOTHERS_MAP = {
    "raw_OLD":           (identity_OLD, lambda d: d - timedelta(days=7)),
    "smoothed_OLD":      (seven_day_moving_average, lambda d: d),
    "raw":               (Smoother("identity", impute_method=None), ""),
    "smoothed":          (Smoother("moving_average", window_length=7), "_7dav")
}



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
