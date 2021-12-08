# -*- coding: utf-8 -*-
"""Functions for pulling data from the USAFacts website."""
from datetime import date
import hashlib
from logging import Logger
import os

import numpy as np
import pandas as pd
import requests
import json

from .constants import DROP_COLUMNS, BASE_URL

def fetch(upload_start_date, upload_end_date, export_start_date, export_end_date):
    url = construct_url(upload_start_date, upload_end_date, export_start_date, export_end_date)
    page = requests.get(url)
    data = pd.DataFrame(json.loads(page.text))

    return(data)

def construct_url(upload_start_date, upload_end_date, export_start_date, export_end_date):
   # Add 1 day to end dates.
   pass
