"""A script to run the flagging module using data from the API."""
from datetime import date, timedelta
import covidcast
import pandas as pd
from delphi_utils.flagging.run import flagger_from_params


#A dictionary of data sources and data signals
source_sig = {
        "google-symptoms":["s01_raw_search", "s01_smoothed_search"],
        "chng":["smoothed_outpatient_cli", "smoothed_adj_outpatient_cli"],
        "jhu-csse":["confirmed_incidence_num", "confirmed_incidence_prop",
                    "confirmed_7dav_incidence_num"],
        "fb-survey":["smoothed_wwearing_mask_7d", "smoothed_wcli",
                     "raw_wcli"],
        "doctor-visits":["smoothed_adj_cli"],
        "quidel":["covid_ag_smoothed_pct_positive"]
}

#Use the same parameters
# with open('params.json') as json_file:
#     pjson = json.load(json_file)



cols = pd.read_csv("./tests/ref_files/basic.csv").columns.to_list()[1:]
#Create input df and run flagging module
for key, value_list in source_sig.items():
    for value in value_list:
        all_lags = pd.DataFrame()
        try:
            all_lags = pd.read_csv(f'./all_lags_{key}-{value}.csv')
        except FileNotFoundError:
            for lag in [0, 1, 2, 4, 8, 16, 32, 60]:
                print(f'"\t\traw_df": "./{key}_{value}2.csv", ' +
                      f'\n\t\t"sig_fold": "{key}", \n\t\t"sig_str"' +
                      f': "{value}",')

                pjson = {
                    "common": {
                        "export_dir": "./receiving",
                        "log_filename": "dfs.log"
                    },
                    "flagging": {
                        "n_train": 200,
                        "ar_lags": 7,
                        "df_start_date": "05/12/2021",
                        "df_end_date": "05/12/2022",

                        "resid_start_date": "01/12/2022",
                        "resid_end_date": "04/12/2022",
                        "eval_start_date": "04/12/2022",
                        "eval_end_date": "05/12/2022",

                        "input_dir": "./receiving",

                        "output_dir": "./",
                        "lags": [60],

                        "raw_df": f'./all_lags_{key}-{value}.csv',
                        "sig_fold": key,
                        "sig_str": value,

                        "sig_type": "api",
                        "remote": True,
                        "flagger_type": "flagger_df"
                    }
                }

                tot_df = pd.DataFrame()
                data_sample = covidcast.signal(key, value,
                                    date(2021,5, 12), date(2021,5, 12), "state")
                for date_s in pd.date_range( date(2021,5, 12), date(2022, 5,12)):
                    data = covidcast.signal(key, value,date_s, date_s,
                                            "state",as_of=date_s-timedelta(lag))
                    if data is not None:
                        data = data[['geo_value', 'value', 'time_value']]
                        data.columns=['state', 'value', 'index']
                    else:
                        data = pd.DataFrame()
                        data['state'] = cols
                        data['index'] = date_s
                        data['value'] = None
                    tot_df = pd.concat([tot_df, data])
                if not tot_df.empty:
                    tot_df = tot_df.set_index(["index", 'state'])
                    tot_df = tot_df.unstack(level=0)
                    tot_df.columns = tot_df.columns.droplevel()
                    tot_df = tot_df.T
                    tot_df['lag'] = lag
                all_lags = pd.concat([tot_df, all_lags], axis=0)
        flagger_from_params(pjson)
