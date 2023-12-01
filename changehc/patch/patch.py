from datetime import datetime, timedelta
import json
import subprocess
from os import makedirs

with open('params.json', 'r') as file:
        data = json.load(file)

SOURCE = "chng"
export_dir = data['common']['export_dir']
makedirs(export_dir, exist_ok=True)

START_DATE = datetime(2022, 12, 29)#20230220
END_DATE = datetime(2022, 12, 29)

current_date = START_DATE
while current_date <= END_DATE:
        date_str = str(current_date.strftime("%Y%m%d"))
        print(date_str)

        issue_dir = "issue_%s" % date_str
        makedirs(f"{export_dir}/{issue_dir}/{SOURCE}", exist_ok=True) #create issue & source dir

        drop_date = current_date - timedelta(days=1)
        data['indicator']['drop_date'] = str(drop_date.strftime("%Y-%m-%d"))
        data['common']['export_dir'] = export_dir + "/" + issue_dir + "/" + SOURCE
        with open('params.json', 'w') as file:
            json.dump(data, file, indent=4)
        subprocess.run("env/bin/python -m delphi_changehc", shell=True)
        current_date += timedelta(days=1)