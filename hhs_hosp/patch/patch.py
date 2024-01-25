from datetime import datetime, timedelta
import json
import subprocess
from os import makedirs
from delphi_utils import read_params
from delphi_hhs.run import run_module


params = read_params()
SOURCE = "hhs"
export_dir = "25"
makedirs(export_dir, exist_ok=True)

START_DATE = datetime(2023, 5, 25)
END_DATE = datetime(2023, 5, 25)

current_date = START_DATE
while current_date <= END_DATE:
    date_str = str(current_date.strftime("%Y%m%d"))
    print(date_str)

    issue_dir = "issue_%s" % date_str
    makedirs(f"{export_dir}/{issue_dir}/{SOURCE}", exist_ok=True) #create issue & source dir

    params['common']['epidata']['as_of'] = date_str
    params['common']['export_dir'] = f"{export_dir}/{issue_dir}/{SOURCE}"

    run_module(params)
    print(f"completed run for issue_{issue_dir}")
    current_date += timedelta(days=1)