"Test the flag_data module"
from delphi_changehc.flag_data import flag_dfs

def test_differet_types():
    "Test flagger on ratio, counts, and api data."
    params = {
  "common": {
    "export_dir": "./receiving",
    "log_filename": "./chng.log",
    "log_exceptions": False
  },
  "flagging_meta": {"generate_dates": True,
                    "n_train": 2,
                    "ar_lags": 1,
                    "remote": False,
                    "output_dir": "./flag_output",
                    "flagger_type": ""
  },
  "flagging": [{
        "lags": ["var"],
        "sig_fold": "chng",
        "sig_str": "smoothed_outpatient_cli",
        "sig_type": "api"
      },
      {"input_dir": "./cache",
        "lags": [20, 30],
        # "raw_df": ".ref_files/covid_raw.csv",
        "sig_fold": "chng",
        "sig_str": "Covid.dat.gz",
        "sig_type": "raw",
      },
      {
        "lags": [20, 30],
        # "raw_df": ".ref_files/covid_ratio.csv",
        "sig_fold": "chng",
        "sig_str":  "Covid.dat.gz",
        "sig_den": "Denom.dat.gz",
        # "flagger_type": "",
        "sig_type": "ratio",
      }
    ]
}
    output = flag_dfs(params)
    names = ['api', 'raw', 'ratio']
    for i, fg in enumerate(output):
        fg.to_csv(f"{names[i]}.csv")
    #create the three dfs
    #save them as tests
    #copmare output to them
    #compare three df lists to each other
    #check if runner output is the same

