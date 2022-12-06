# Change Flagging System

THIS README IS IN PROGRESS

FlaSH is a real-time point-outlier detection system. We add the daily evaluation step to this indicators package (retraining is done offline).

FlaSH produces a list of data points that are unusual or surprising so that stakeholders are aware of points that warrant further inspection. 

The guiding principles for the system are: 
- Flag relevant data points as soon as possible (ideally in an online setting)
- Be aware of the false positive/false negative rates
- Reduce cognitive load on data evaluators 

Types of outliers/changes FlaSH intends to catch are: 
- Out-of-range points 
- Large spikes 
- Points that are interesting for a particular weekday
- Points that are interesting with respect to a particular stream's history
- Points that are interesting with respect to all other streams 
- Change in data reporting schedule
- Changes in health condition [ex: new variant]

## Running FlaSH-eval
In params.json, there are two parameters: flagging_meta which is a dictionary and flagging, which is a list of dictionaries. 
Two key parameters in flagging_meta are:
1. **remote**: True/False
Are you running this system so that it's checking against the s3 filesystem or a local one? 

True = S3, False = Local 

Related Parameters: output_dir, input_dir

2. **flagger_type**: flagger_df/flagger_io

File regeneration is time-consuming. Should you regenerate all files specified or only those that need to be updated. For example, between different runs where only AR parameters are changed, the reference files can stay the same. 

flagger_df: Regenerate All, flagger_io: Regenerate only necessary files 


There are a few different ways to run the flagger. 

1. What input data are you using? 
Types: "api", "raw", "ratio" 
- API: In params.json, flagging, set "sig_type" to "api", and the dataframe will be generated.
- Raw/Ratio: Create a file, flag_data.py, in delphi_* for the indicator of interest that handles different sig_types as expected. See changehc/delphi_changehc/flag_data.py
- Existing csv: Point to relevant location in 'raw_df' location

- The dataframe should be as follows: 

**Columns**: State Abbreviations [ak, ny, tx ...] & Lag Type. Total of 51 columns

**Index**: Dates 

So a sample dataframe would look like this:

| -          | ak  | ny  | tx  | lags |
|------------|-----|-----|-----|------|
| 2021-12-03 | 100 | 123 | 45  | 1    |
| 2021-12-04 | 30  | 20  | 78  | 1    |
| 2021-12-03 | 300 | 323 | 90  | 2    |
| 2021-12-04 | 90  | 40  | 100 | 2    |


To run the flagging system, follow similar instructions as the validator readme copied below:

You can excecute the Python module contained in this
directory from the main directory of the indicator of interest.

The safest way to do this is to create a virtual environment,
and install the common DELPHI tools, including the flagger, and the
flagging module and its dependencies to the virtual environment.

To do this, navigate to the main directory of the indicator of interest and run the following code:

```
python -m venv env
source env/bin/activate
pip install ../_delphi_utils_python/.
pip install .
```

To execute the module run the indicator to generate data files , 
then run a module to put them in the correct dataframe, and finally then run
the flagging system , as follows:

```
env/bin/python -m delphi_INDICATORNAME
env/bin/python create_df_process.py #this is up to you!
env/bin/python -m delphi_utils.flagging
```

Once you are finished with the code, you can deactivate the virtual environment
and (optionally) remove the environment itself.

```
deactivate
rm -r env
```

You have a lot of flexibility for new functionality of the flagging module. 

### Customization

All of the user-changable parameters are stored in the `flagging` field of the indicator's `params.json` file. If `params.json` does not already include a `flagging` field, please copy that provided in this module's `params.json.template`.

Please update the follow settings:
- flagging_meta
  - "generate_dates": determines dates for parameters in flagging (below) are recreated daily
  - "aws_access_key_id": for remote options,
  - "aws_secret_access_key": for remote options,
  - "n_train": the number of days used for training
  - "ar_lags": the number of days used for the lag
  - "ar_type": what type of autoregressive model do you want to use [TODO]
  - "output_dir": location where files will be saved if using local filesystem 
  - "flagger_type": flagger_df to regenerate all files or flagger_io to regenerate just the missing files
- flagging: a list of dictionaries each with some of these params
  - "df_start_date": start date of dataframe (used to create input df)
  - "df_end_date": end date of dataframe (used to create input df)
  - "resid_start_date": used to create the residual distribution 
  - "resid_end_date": used to create the residual distribution 
  - "eval_start_date": date range to create flags 
  - "eval_end_date": date range to create flags 
  - "sig_str": usually the signal name, used to create/save files
  - "sig_fold": the name of the data source for organizational purposes
  - "sig_type": the type of signal (raw, api, ratio) for organizational purposes
  - "remote": are you using the local or S3 filesystem 
  - "lags": how many lags do you want to consider. Consider if your signal does have lags and the role of backfill per signal 
  - "raw_df": the location of the input dataframe
  - "input_dir": location of relevant files to create the raw df



## Testing the code

To test the code, please create a new virtual environment in the main module directory using the following procedure, similar to above:

```
make install
```

To do a static test of the code style, it is recommended to run **pylint** on
the module. To do this, run the following from the main module directory:

```
make lint
```

The most aggressive checks are turned off; only relatively important issues
should be raised and they should be manually checked (or better, fixed).

Unit tests are also included in the module. To execute these, run the following command from this directory:

```
make test
```

or 

```
(cd tests && ../env/bin/pytest test_file.py --cov=delphi_utils.flagging --cov-report=term-missing)
```

The output will show the number of unit tests that passed and failed, along with the percentage of code covered by the tests. None of the tests should fail and the code lines that are not covered by unit tests should be small and should not include critical sub-routines.


## Code tour
* run.py: sends params.json fields to and runs the validation process
* generate_reference.py: generates the reference files related to a specific run 
* generate_ar.py: generates the ar files related to a specific run 
* flag_io.py: various functions to figure out which files need to be generated with specific parameters.
* flag_data.py (local): generates the input dataframe (see application in runner.py)
## Adding checks

To add a new validation check. Each check should append a descriptive error message to the `raised` attribute if triggered. All checks should allow the user to override exception raising for a specific file using the `suppressed_errors` setting in `params.json`.
