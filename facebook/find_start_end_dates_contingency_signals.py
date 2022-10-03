import glob
import pandas as pd
from datetime import datetime


input_files = glob.glob("Downloads/20220927_contingency_backfill/weekly/*nation_all_indicators_overall.csv.gz")
input_files.sort()

results = dict()
for file in input_files:
	print(file)
	data = pd.read_csv(file, compression="gzip")
	start_date = datetime.strptime(str(data.period_start[0]), "%Y%m%d").date()
	end_date = datetime.strptime(str(data.period_end[0]), "%Y%m%d").date()

	for col in data.columns:
		# Skip columns that appear in all files. Keep issue_date as common-sense check
		if col in ['survey_geo', 'period_start', 'period_end', 'period_val', 'period_type',
	   'geo_type', 'aggregation_type', 'country', 'ISO_3', 'GID_0', 'region',
	   'GID_1', 'state', 'state_fips', 'county', 'county_fips']:
			continue
		if col not in results.keys():
			results[col] = (start_date, end_date)
		else:
			old_start = results[col][0]
			old_end = results[col][1]
			results[col] = (min(start_date, old_start), max(end_date, old_end))

pd.DataFrame.from_dict(
	results, orient="index"
).reset_index(
).rename(
	columns={"index":"signal",0:"start_date", 1:"end_date"}
).sort_values("signal"
).to_csv(
	"Downloads/contingency_signal_dates.csv",
	index=False
)
