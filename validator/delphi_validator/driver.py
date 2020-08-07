from fbsurveyvalidation import *
from datafetcher import *

# Dev Alert: Remove template extention
params = read_params("..\params.json.template")
dtobj_sdate = datetime.strptime(params['start_date'], '%Y-%m-%d')
dtobj_edate = datetime.strptime(params['end_date'], '%Y-%m-%d')

# Collecting all filenames
daily_filnames = read_filenames("../data")

fbsurvey_validation(daily_filnames, dtobj_sdate, dtobj_edate)
