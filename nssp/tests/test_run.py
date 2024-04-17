from datetime import datetime, date
import json
from unittest.mock import patch
import tempfile
import os
import time
from datetime import datetime

import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal
from delphi_utils import S3ArchiveDiffer, get_structured_logger, create_export_csv, Nans

from delphi_nssp.constants import GEOS, METRICS, CSV_COLS, SENSORS
from delphi_nssp.run import (
    add_needed_columns
)

def test_add_needed_columns():
    df = pd.DataFrame({'geo_id': ['us'], 'val': [1]})
    df = add_needed_columns(df, col_names=None)
    assert df.columns.tolist() == [
        "geo_id","val", "se", "sample_size",
        "missing_val", "missing_se", "missing_sample_size"]

    assert df["se"].isnull().all()
    assert df["sample_size"].isnull().all()

