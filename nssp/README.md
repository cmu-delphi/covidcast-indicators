# NSSP Emergency Department Visit data

We import the NSSP Emergency Department Visit data, currently only the smoothed concentration, from the CDC website, aggregate to the state and national level from the wastewater sample site level, and export the aggregated data.
For details see the `DETAILS.md` file in this directory.

## Create a MyAppToken
`MyAppToken` is required when fetching data from SODA Consumer API 
(https://dev.socrata.com/foundry/data.cdc.gov/r8kw-7aab). Follow the 
steps below to create a MyAppToken.
- Click the `Sign up for an app token` button in the linked website
- Sign In or Sign Up with Socrata ID
- Click the `Create New App Token` button
- Fill in `Application Name` and `Description` (You can just use delphi_wastewater
  for both) and click `Save`
- Copy the `App Token`


## Running the Indicator

The indicator is run by directly executing the Python module contained in this
directory. The safest way to do this is to create a virtual environment,
installed the common DELPHI tools, and then install the module and its
dependencies. To do this, run the following command from this directory:

```
make install
```

This command will install the package in editable mode, so you can make changes that
will automatically propagate to the installed package. 

All of the user-changable parameters are stored in `params.json`. To execute
the module and produce the output datasets (by default, in `receiving`), run
the following:

```
env/bin/python -m delphi_nssp
```

If you want to enter the virtual environment in your shell, 
you can run `source env/bin/activate`. Run `deactivate` to leave the virtual environment. 

Once you are finished, you can remove the virtual environment and 
params file with the following:

```
make clean
```

## Testing the code

To run static tests of the code style, run the following command:

```
make lint
```

Unit tests are also included in the module. To execute these, run the following
command from this directory:

```
make test
```

To run individual tests, run the following:

```
(cd tests && ../env/bin/pytest <your_test>.py --cov=delphi_NAME --cov-report=term-missing)
```

The output will show the number of unit tests that passed and failed, along
with the percentage of code covered by the tests. 

None of the linting or unit tests should fail, and the code lines that are not covered by unit tests should be small and
should not include critical sub-routines. 

## Running Patches:
A daily backup of from source in the form of csv files can be found on `bigchunk-dev-02` under `/common/source_backup/nssp`. Talk to your sysadmin for access. 

You can also generate your own backup from source by setting up a cron job that runs the following .py every day when a pipeline outtage is going on on our side but aource api is still available:
```
import numpy as np
import pandas as pd
from sodapy import Socrata
from datetime import date

today = date.today()
socrata_token = 'FILL_YOUR_OWN_TOKEN_HERE'
client = Socrata("data.cdc.gov", socrata_token)
results = []
offset = 0
limit = 50000  # maximum limit allowed by SODA 2.0
while True:
    page = client.get("rdmq-nq56", limit=limit, offset=offset)
    if not page:
        break  # exit the loop if no more results
    results.extend(page)
    offset += limit
df_ervisits = pd.DataFrame.from_records(results)
df_ervisits.to_csv(f'~/{today}.csv', index=False)
```
When you're ready to create patching data for a specific date range in batch issue format, adjust `params.json` in accordance with instructions in `patch.py`, move the backup csv files into your chosen `source_dir`, then run
```
env/bin/python -m delphi_nssp.patch
```