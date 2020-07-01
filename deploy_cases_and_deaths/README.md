# Combined Cases and Deaths

We create a combined cases and deaths signal for visualization only (not available in covidcast API). 
It includes all of the information in usa-facts and Puerto Rico only from jhu-csse.

## Running the Indicator

The indicator is run by directly executing the Python script run.py. 
The covidcast API need to be installed first. 
To do this, run the following code:

```
pip install covidcast
```

To execute the script and produce the output datasets (by default, in `receiving`), run
the following:

```
python run.py
```
By default, the script will generate the combined signal for the most recent data only (usually for yesterday only).
If you want to produce the combined signal for all the dates back to the first valid date, run the following:
```
python run.py --date_range all
```
If you want to set a specific date range, run the following:
```
python run.py --date_range yyyymmdd-yyyymmdd
```


