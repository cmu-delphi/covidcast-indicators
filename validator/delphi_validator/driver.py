from fbsurveyvalidation import *
from datafetcher import *

# Defining start date and end date for the last fb-survey pipeline execution
survey_sdate = "2020-06-13"
survey_edate = "2020-06-15"

# Dev Alert: Remove template extention
params = read_params("..\params.json.template")
dtobj_sdate = datetime.strptime(params['start_date'], '%Y-%m-%d')
dtobj_edate = datetime.strptime(params['end_date'], '%Y-%m-%d')

# Collecting all filenames
daily_filnames = read_filenames("../data")

fbsurvey_validation(daily_filnames, dtobj_sdate, dtobj_edate)
