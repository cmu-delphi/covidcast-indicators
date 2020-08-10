# Validator

The validator performs two main tasks:
1) Sanity checks on daily data generated from a pipeline of specific data
   source. 
2) Its does a comparative analysis with recent data from the API 
   to detect any anomalies such as spikes, significant value differences

The validator validates against daily data thats already written in the disk
making the execution of the validator independent of the pipeline execution.
This creates an additional advantage of running the validation against multiple
days of daily data and have a better cummulative analysis.


## Running the Indicator

The indicator is run by directly executing the Python module contained in this
directory. The safest way to do this is to create a virtual environment,
installed the common DELPHI tools, and then install the module and its
dependencies. To do this, run the following code from this directory:

```
python -m venv env
source env/bin/activate
pip install ../_delphi_utils_python/.
pip install .
```

All of the user-changable parameters are stored in `params.json`. To execute
the module and produce the output datasets (by default, in `receiving`), run
the following:

```
env/bin/python -m delphi_validator
```

Once you are finished with the code, you can deactivate the virtual environment
and (optionally) remove the environment itself.

```
deactivate
rm -r env
```
