# USA Facts Cases and Deaths

We import the confirmed case and deaths data from USA Facts website and export
the county-level data as-is.  We also aggregate the data to the MSA, HRR, and
State levels. For detailed information see the files `DETAILS.md` contained
in this directory.

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
env/bin/python -m delphi_usafacts
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
env/bin/pylint delphi_usafacts
```

The most aggressive checks are turned off; only relatively important issues
should be raised and they should be manually checked (or better, fixed).

Unit tests are also included in the module. To execute these, run the following
command from this directory:

```
(cd tests && ../env/bin/pytest --cov=delphi_usafacts --cov-report=term-missing)
```

The output will show the number of unit tests that passed and failed, along
with the percentage of code covered by the tests. None of the tests should
fail and the code lines that are not covered by unit tests should be small and
should not include critical sub-routines.

- Jenkins test #1
