#This file is in-progress, but has


# #This is a file which uses the json file to come up with the calling code
# from .flag_io import flagger
#
# def calling_code():
#     #get parameters from the json file
#     start_time = time.time()
#     logger = get_structured_logger(
#         __name__, filename=params["common"].get("log_filename"),
#         log_exceptions=params["common"].get("log_exceptions", True))
#     cache_dir = params["indicator"]["input_cache_dir"]
#     export_dir = params["common"]["export_dir"]
#     lags = params["indicator"]["lags"]
#     num_lags = params["indicator"]["num_lags"]
#     n_train = params["indicator"]["n_train"]
#     n_test = params["indicator"]["n_test"]
#     n_valid = params["indicator"]["n_valid"]
#     if raw:
#         processing_fn_raw()
#     else:
#         processing_fn_api()
#
#
# def processing_fn_raw(input_dir, start_date, end_date, lag):
#     continue
#
#
# def processing_fn_api(start_date, end_date, lag):
#     continue
#
#     input_df =  # TODO: investigate the keys
#
#
#     start_date = pd.to_datetime(params["indicator"]["start_date"])
#     end_date = pd.to_datetime(params["indicator"]["end_date"])
#     assert num_lags < n_train, \
#         "The number of lags you use for the AR model has to \
#         be less than the number of samples you train on."
#     assert start_date <= end_date, \
#         "Start date cannot exceed end_date"
#     # TODO: change assert statements per signal!
#     # TODO: Other validation statements.
#     # The valid + test + train has to be <= than the total date range
#     assert start_date > pd.to_datetime("03/01/2020"), \
#         "Start date must be after March 1st, 2020"
#
#
#
def pull_lags_data(cache_dir, lags, start_date, end_date, reset=False):
    """Import num & den counts files and update as needed using data from the SFTP."""

    def make_df(file_names, lags):
        # TO DO: Make clear that this is only for CHC data
        """Create the lags from each input filename."""
        df_list = []
        for file_date in file_names.index:
            df = file_names.loc[file_date, :]
            new_df = pd.read_csv(df.fname, header=None, dtype={
                0: 'str',
                1: 'str',
                2: 'str'
            }, parse_dates=[0])
            new_df.columns = ['date', 'state', 'counts']
            new_df['date'] = pd.to_datetime(new_df['date'], format='%Y%m%d', errors='coerce')
            const_day = 10  # a threshold we never want to go below for the adjusted lags
            # threshold is at least lag size
            dates_dict = {}
            for x in lags:
                tmp_date = file_date - pd.Timedelta(days=df.win_sub +
                                                         max(x - df.win_sub, min(const_day, x)))
                dates_dict[tmp_date] = x
            new_df = new_df[new_df.date.isin(dates_dict.keys())]
            new_df['lags'] = new_df['date'].map(dates_dict)
            new_df = new_df[pd.to_numeric(new_df['state'], errors='coerce').notnull()]
            new_df['state'] = new_df.state.astype(int).astype(str).str.zfill(5).str[:2].astype(int)
            new_df = new_df.query('state<57')
            new_df['counts'] = new_df['counts'].replace("3 or less", 2).astype(int)
            new_df = new_df.drop(columns=['date']).groupby(['lags', 'state']). \
                sum().sort_values(by=['lags', 'state'])
            new_df.columns = [file_date]
            df_list.append(new_df)
        tmp = pd.concat(df_list, axis=1)
        return tmp

    def file_update(cache_dir, str_file, lags, start_date, end_date, reset=False):
        """Determine data needed and place preprocessed data nicely into an output dataframe."""
        if end_date is None:
            end_date = date.today()
        assert end_date.date() <= date.today(), \
            "End date cannot exceed today's date"
        dates_range = pd.date_range(start_date, end_date)
        df = pd.DataFrame(columns=['lags', 'state'])
        list_fname = glob.glob(f'{cache_dir}/{str_file}.csv')
        if len(list_fname) == 1 and not reset:
            df = pd.read_csv(list_fname[0], header=0).fillna(0).astype(int)
        existing_lags = np.unique(df['lags']).astype(int)
        df = df.set_index(['lags', 'state'])
        df.columns = pd.to_datetime(df.columns)
        existing_dates = list(filter(lambda x: x in dates_range, df.columns))
        missing_lags = list(filter(lambda x: x not in existing_lags, lags))
        missing_dates = list(filter(lambda x: pd.to_datetime(x,
                                                             format="%Y%m%d") not in existing_dates, dates_range))

        rel_files = pd.DataFrame()
        rel_files['fname'] = glob.glob(f'{cache_dir}/*{str_file}.dat.gz')
        rel_files['fdate'] = pd.to_datetime(
            rel_files['fname'].str.rsplit('/', n=1, expand=True)[1].
                str.split('_', n=1, expand=True)[0],
            format='%Y%m%d', errors='coerce')
        rel_files = rel_files.set_index('fdate')
        merge_files = pd.DataFrame(index=dates_range)
        rel_files = merge_files.merge(rel_files, how='outer', left_index=True,
                                      right_index=True).fillna(method='ffill')
        rel_files['win_sub'] = list(rel_files.reset_index(drop=True).groupby(['fname']).cumcount())
        if (len(missing_lags) > 0) and len(existing_dates) > 0:
            sel_rel_files = rel_files.query('index in @existing_dates').sort_index()
            df = pd.concat([df, make_df(sel_rel_files, missing_lags)]).fillna(0)
        sel_rel_files = rel_files[rel_files.index.isin(missing_dates)].sort_index()
        if sel_rel_files.shape[0] > 0:
            df = pd.concat([df, make_df(sel_rel_files, lags)], axis=1).fillna(0)
        df.to_csv(f'{cache_dir}/{str_file}.csv')
        return df[dates_range]

    df_num = file_update(cache_dir, 'Covid', lags, start_date, end_date, reset)
    df_den = file_update(cache_dir, 'Denom', lags, start_date, end_date, reset)
    return df_num, df_den