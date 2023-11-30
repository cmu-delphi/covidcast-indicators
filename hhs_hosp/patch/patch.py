from datetime import datetime, timedelta
import json
import subprocess
import os

with open('params.json', 'r') as file:
        data = json.load(file)

SOURCE = "hhs"
export_dir = "25"
os.mkdir(export_dir)

START_DATE = datetime(2023, 5, 25)
END_DATE = datetime(2023, 5, 25)

current_date = START_DATE
while current_date <= END_DATE:
    date_str = str(current_date.strftime("%Y%m%d"))
    print(date_str)

    issue_dir = "issue_%s" % date_str
    os.mkdir("%s/%s" % (export_dir,issue_dir)) #create issue dir
    os.mkdir("%s/%s/%s" % (export_dir,issue_dir,SOURCE)) #create source dir
    
    data['common']['epidata']['as_of'] = date_str
    data['common']['export_dir'] = export_dir + "/" + issue_dir + "/" + SOURCE
    with open('params.json', 'w') as file:
        json.dump(data, file, indent=4)
    subprocess.run("env/bin/python -m delphi_hhs", shell=True)
    current_date += timedelta(days=1)