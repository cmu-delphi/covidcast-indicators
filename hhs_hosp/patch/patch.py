from datetime import datetime, timedelta
import json
import subprocess
from os import makedirs

with open('params.json', 'r') as file:
        data = json.load(file)

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

    data['common']['epidata']['as_of'] = date_str
    data['common']['export_dir'] = export_dir + "/" + issue_dir + "/" + SOURCE
    with open('params.json', 'w') as file:
        json.dump(data, file, indent=4)
    subprocess.run("env/bin/python -m delphi_hhs", shell=True)
    current_date += timedelta(days=1)