# Patterns Data in Safegraph Mobility Datasets

We import raw mobility data from Safegraph Weekly Patterns, calculate some 
statistics upon it, and aggregate the data from the Zip Code level to County,
HRR, MSA and State levels. For detailed information see the files `DETAILS.md` 
contained in this directory.

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

One must also install the
[AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-welcome.html).
Please refer to OS-specific instructions to install this command line
interface, and verify that it is installed by calling `which aws`.
If `aws` is not installed prior to running the pipeline, it will raise
a `FileNotFoundError`.

All of the user-changable parameters are stored in `params.json`. To execute
the module and produce the output datasets (by default, in `receiving`), run
the following:

```
env/bin/python -m delphi_safegraph_patterns
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
env/bin/pylint delphi_safegraph_patterns
```

The most aggressive checks are turned off; only relatively important issues
should be raised and they should be manually checked (or better, fixed).

Unit tests are also included in the module. To execute these, run the following
command from this directory:

```
(cd tests && ../env/bin/pytest --cov=delphi_safegraph_patterns --cov-report=term-missing)
```

The output will show the number of unit tests that passed and failed, along
with the percentage of code covered by the tests. None of the tests should
fail and the code lines that are not covered by unit tests should be small and
should not include critical sub-routines.
