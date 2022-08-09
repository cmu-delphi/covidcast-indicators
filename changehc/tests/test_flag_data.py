"Test the flag_data module"
from delphi_changehc.flag_data import flag_dfs
from pandas.testing import assert_frame_equal
from delphi_utils import runner
import pandas as pd

def test_differet_inputs():
    "Test flagger on ratio, counts, and api data."
    params = {
  "common": {
    "export_dir": "./receiving",
    "log_filename": "./chng.log",
    "log_exceptions": False
  },
  "flagging_meta": {"generate_dates": False,
                    "n_train": 2,
                    "ar_lags": 1,
                    "remote": False,
                    "output_dir": "./flag_output",
                    "flagger_type": ""
  },
  "flagging": [{
       "df_start_date": "04/01/2022",
       "df_end_date": "04/10/2022",
       "resid_start_date": "04/04/2022",
       "resid_end_date": "04/07/2022",
       "eval_start_date": "04/08/2022",
       "eval_end_date": "04/09/2022",
        "lags": ["var"],
        "sig_fold": "chng",
        "sig_str": "smoothed_outpatient_cli",
        "sig_type": "api"
      },
      {"input_dir": "./cache2",
        "lags": [20, 25],
       "df_start_date": "04/01/2022",
       "df_end_date": "04/10/2022",
       "resid_start_date": "04/04/2022",
       "resid_end_date": "04/07/2022",
       "eval_start_date": "04/08/2022",
       "eval_end_date": "04/09/2022",
        "sig_fold": "chng",
        "sig_str": "Covid.dat.gz",
        "sig_type": "raw",
      },
      {"input_dir": "./cache2",
        "lags": [20, 25],
       "df_start_date": "04/01/2022",
       "df_end_date": "04/10/2022",
       "resid_start_date": "04/04/2022",
       "resid_end_date": "04/07/2022",
       "eval_start_date": "04/08/2022",
       "eval_end_date": "04/09/2022",
        "sig_fold": "chng",
        "sig_str":  "Covid.dat.gz",
        "sig_den": "Denom.dat.gz",
        "sig_type": "ratio",
      }
    ]
}
    params2, output = flag_dfs(params)
    names = ['api', 'raw', 'ratio']
    for i, fg in enumerate(output):
        fg.columns.name = None
        assert_frame_equal(fg,
                           pd.read_csv(f'./ref_csv/{names[i]}.csv',
                                       index_col=0, parse_dates=[0]))


