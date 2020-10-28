# CDC COVID-NET Indicator



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

All of the user-changable parameters are stored in `params.json`.

The module has to request for each participating state's hospitalization data
from the COVID-NET API individually. When `parallel` is set to `true` in
`params.json`, the module makes these requests in parallel (up to 10 at once)
instead of sequentially. This should make downloading all hospitalization data
faster, especially for larger number of participating states.

To execute
the module and produce the output datasets (by default, in `receiving`), run
the following:

```
env/bin/python -m delphi_cdc_covidnet
```

Once you are finished with the code, you can deactivate the virtual environment
and (optionally) remove the environment itself.

```
deactivate
rm -r env
```

## Testing the code

To do a static test of the code style, it is recommended to run **pylint** on
the module. To do this, run the following from the main module directory:

```
env/bin/pylint delphi_cdc_covidnet
```

The most aggressive checks are turned off; only relatively important issues
should be raised and they should be manually checked (or better, fixed).

Unit tests are also included in the module. To execute these, run the following
command from this directory:
(Note: the following command requires python 3.8, having any version less than 3.8 might
fail some test cases. Please install it before running.)
```
(cd tests && ../env/bin/pytest --cov=delphi_cdc_covidnet --cov-report=term-missing)
```

The output will show the number of unit tests that passed and failed, along
with the percentage of code covered by the tests. None of the tests should
fail and the code lines that are not covered by unit tests should be small and
should not include critical sub-routines.
