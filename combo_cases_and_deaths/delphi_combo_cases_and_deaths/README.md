# Combined Cases and Deaths

We create a combined cases and deaths signal for visualization only (not available in covidcast API). 
It includes all of the information in usa-facts and Puerto Rico only from jhu-csse.

## Running the Indicator

The indicator is run by directly executing the Python script run.py. 
The safest way to do this is to create a virtual environment,
installed the common DELPHI tools, and then install the module and its
dependencies. To do this, run the following code from this directory:

```
python -m venv env
source env/bin/activate
pip install ../_delphi_utils_python/.
pip install covidcast
```

To execute the script and produce the output datasets (by default, in `receiving`), run
the following:

```
env/bin/python run.py
```
By default, the script will generate the combined signal for the most recent data only (usually for yesterday only).
If you want to produce the combined signal for all the dates back to the first valid date, run the following:
```
env/bin/python run.py --date_range all
```
If you want to set a specific date range, run the following:
```
env/bin/python run.py --date_range yyyymmdd-yyyymmdd
```

Once you are finished with the code, you can deactivate the virtual environment and (optionally) remove the environment itself.
```
deactivate
rm -r env
```

