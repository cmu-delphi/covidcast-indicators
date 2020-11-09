# Sir Complains-a-Lot

Sir Complains-a-Lot uses the COVIDcast API to determine if any indicators have
not been updated in a preset period of time, and complains on Slack if so.

The bot is run by directly executing the Python module contained in this
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
env/bin/python -m delphi_sir_complainsalot
```

Once you are finished with the code, you can deactivate the virtual environment
and (optionally) remove the environment itself.

```
deactivate
rm -r env
```

## Specifying sources and maintainers

See the current `params.json` for examples. To specify maintainers for a source,
you need their Slack member ID, available under the "More" menu in their
profile; Slack uses this to uniquely identify users and link to their names
correctly, regardless of their display name.

## Testing the code

To do a static test of the code style, it is recommended to run **pylint** on
the module. To do this, run the following from the main module directory:

```
env/bin/pylint delphi_sir_complainsalot
```

The most aggressive checks are turned off; only relatively important issues
should be raised and they should be manually checked (or better, fixed).
