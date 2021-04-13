# DELPHI Common Utility Functions (Python)

This directory contains the Python module `delphi_utils`. It includes a number of
common functions that are useful across multiple indicators.

## Installing the Module

To install the module in your default version of Python, run the
following from this directory:

```
pip install .
```

As described in each of the indicator code directories, you will want to install
this module within a virtual environment when testing the various code bases.

### Testing the code

To do a static test of the code style, it is recommended to run **pylint** on
the module. To do this, run the following from the main module directory:

```
pylint delphi_utils
```

The most aggressive checks are turned off; only relatively important issues
should be raised and they should be manually checked (or better, fixed).

Unit tests are also included in the module. These should be run by first
installing the module into a virtual environment:

```
python -m venv env
source env/bin/activate
pip install .
```

And then running the unit tests with:

```
(cd tests && ../env/bin/pytest --cov=delphi_utils --cov-report=term-missing)
```

The output will show the number of unit tests that passed and failed, along
with the percentage of code covered by the tests. None of the tests should
fail and the code lines that are not covered by unit tests should be small and
should not include critical sub-routines.

When you are finished, the virtual environment can be deactivated and
(optionally) removed.

```
deactivate
rm -r env
```
