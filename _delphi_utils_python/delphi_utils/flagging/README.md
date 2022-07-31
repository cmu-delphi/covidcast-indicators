# Change Flagging System

IN PROGRESS

The flagging system flags data points that are unusual or surprising 
so that stakeholders are aware of changes in the data. 
There are a few key guiding principles for the system: 
- We want to flag relevant data points as soon as possible (ideally in an online setting)
- We want to be aware of the false positive/false negative rates
- We want to reduce cognitive load on data evaluators 

The system determines if any new data points warrants further inspection. 

The validator works both with smoothed historical data from the [COVIDcast API](https://cmu-delphi.github.io/delphi-epidata/api/covidcast.html)
as well as the raw historical data from Epidata. 



## Running the Flagging System

There are three important dimensions to the flagging system:

**Params**
1. **remote**: True/False

Are you running this system so that it's checking against the s3 filesystem or a local one? 

True = S3, False = Local 

Related Parameters: output_dir, input_dir

2. **flagger_type**: flagger_df/flagger_io

File regeneration is time-consuming. Should you regenerate all files specified or only those that need to be updated. For example, between different runs where only AR parameters are changed, the reference files can stay the same. 

flagger_df: Regenerate All, flagger_io: Regenerate only necessary files 



3. Use Calling Code/Run Separately:
How should the flagger be used? You can either use a specific module you build for a signal. For example, this code will both create an input dataframe and also directly run the flagging module. Or, you can create the input dataframe and run the module like you do the validator as shown below.

Step 1: Create Input Data Frame

The dataframe should be as follows: 

**Columns**: State Abbreviations [ak, ny, tx ...] & Lag Type. Total of 51 columns

**Index**: Dates 

So a sample dataframe would look like this:

| -          | ak  | ny  | tx  | lags |
|------------|-----|-----|-----|------|
| 2021-12-03 | 100 | 123 | 45  | 1    |
| 2021-12-04 | 30  | 20  | 78  | 1    |
| 2021-12-03 | 300 | 323 | 90  | 2    |
| 2021-12-04 | 90  | 40  | 100 | 2    |


Step 2: Run the flagging System. 

You can create a calling code module within an indicator folder. This file will have __init__.py and __main__.py files.
The module will create a dataframe and use functions from flag_io.py to run the flagging system. 

```
python -m venv env
source env/bin/activate
pip install ../_delphi_utils_python/.
pip install .
env/bin/python -m calling_code
```

You can also executing the Python module contained in this
directory from the main directory of the indicator of interest.

The safest way to do this is to create a virtual environment,
and install the common DELPHI tools, including the validator, and the
validator module and its dependencies to the virtual environment.

To do this, navigate to the main directory of the indicator of interest and run the following code:

```
python -m venv env
source env/bin/activate
pip install ../_delphi_utils_python/.
pip install .
```

To execute the module run the indicator to generate data files  (by default, in `receiving`), 
then run a module to put them in the correct dataframe, and finally then run
the flagging system , as follows:

```
env/bin/python -m delphi_INDICATORNAME
env/bin/python -m delphi_input_frame
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
- flagging
  - "n_train": the number of days used for training
  - "ar_lags": the number of days used for the lag
  - "ar_type": what type of autoregressive model do you want to use [TODO]
  - "df_start_date": start date of dataframe (used to create input df)
  - "df_end_date": end date of dataframe (used to create input df)
  - "resid_start_date": used to create the residual distribution 
  - "resid_end_date": used to create the residual distribution 
  - "eval_start_date": date range to create flags 
  - "eval_end_date": date range to create flags 
  - "sig_str": usually the signal name, used to create/save files
  - "sig_fold": the name of the data source for organizational purposes
  - "sig_type": the type of signal (raw, api, ratio) for organizational purposes
  - "flagger_type": flagger_df to regenerate all files or flagger_io to regenerate just the missing files
  - "remote": are you using the local or S3 filesystem 
  - "lags": how many lags do you want to consider. Consider if your signal does have lags and the role of backfill per signal 
  - "raw_df": the location of the input dataframe 
  - "output_dir": location where files will be saved if using local filesystem 
  - "input_dir": location of relevant files to create the raw df



## Testing the code

To test the code, please create a new virtual environment in the main module directory using the following procedure, similar to above:

```ls 
python -m venv env
source env/bin/activate
pip install ../../../_delphi_utils_python/.
```

To do a static test of the code style, it is recommended to run **pylint** on
the module. To do this, run the following from the main module directory:

```
env/bin/pylint delphi_utils.flagging
```

The most aggressive checks are turned off; only relatively important issues
should be raised and they should be manually checked (or better, fixed).

Unit tests are also included in the module. To execute these, run the following command from this directory:
TODO: Fix this call 
```
(cd tests && ../env/bin/pytest --cov=delphi_utils.flagging --cov-report=term-missing)
```

The output will show the number of unit tests that passed and failed, along with the percentage of code covered by the tests. None of the tests should fail and the code lines that are not covered by unit tests should be small and should not include critical sub-routines.


## Code tour
* run.py: sends params.json fields to and runs the validation process
* generate_reference.py: generates the reference files related to a specific run 
* generate_ar.py: generates the ar files related to a specific run 
* flag_io.py: various functions to figure out which files need to be generated with specific parameters.

## Adding checks

To add a new validation check. Each check should append a descriptive error message to the `raised` attribute if triggered. All checks should allow the user to override exception raising for a specific file using the `suppressed_errors` setting in `params.json`.
