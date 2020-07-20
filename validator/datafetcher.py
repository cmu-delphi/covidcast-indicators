from os import listdir, stat
from os.path import isfile, join 
import platform
import covidcast
from datetime import date, datetime, timedelta


def read_filenames(path):
    daily_filenames = [f for f in listdir(path) if isfile(join(path, f))]
    return daily_filenames


def fetch_daily_data(data_source, survey_date, geo_type, signal):
    data_to_validate = covidcast.signal(data_source, signal, survey_date, survey_date, geo_type)
    return data_to_validate
    

def new_stuff():
    survey_sdate = "2020-06-13"
    survey_edate = "2020-06-20"
    dtobj_sdate = datetime.strptime(survey_sdate, '%Y-%m-%d')
    dtobj_edate = datetime.strptime(survey_edate, '%Y-%m-%d')
    print(dtobj_sdate.date())
    print(dtobj_edate.date())

    number_of_dates = dtobj_edate - dtobj_sdate + timedelta(days=1)
    print(number_of_dates)

    date_seq = {dtobj_sdate + timedelta(days=x) for x in range(number_of_dates.days + 1)}
    print(date_seq)

    # 1) Lets first fetch all daily filenames


    data = covidcast.signal("fb-survey", "raw_ili", date(2020, 6, 19), date(2020, 6, 19),
                            "state")
    #print(data)


    unique_dates = set()
    unique_dates_obj = set()

    for daily_filename in daily_filenames:
        unique_dates.add(daily_filename[0:8])

    for unique_date in unique_dates:
        newdate_obj = datetime.strptime(unique_date, '%Y%m%d')
        unique_dates_obj.add(newdate_obj)

    check_dateholes = date_seq.difference(unique_dates_obj)
    if check_dateholes:
        print("Date holes exist!")
        print(check_dateholes)
  





#print(data)
#print(data.dtypes)

#print(type(data))

#meta = covidcast.metadata()
#meta.to_csv('meta_out.csv')
#print(meta)