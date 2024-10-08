## Code Review (Python)

A code review of this module should include a careful look at the code and the
output. To assist in the process, but certainly not in replace of it, please
check the following items.

**Documentation**

- [ ] the README.md file template is filled out and currently accurate; it is
possible to load and test the code using only the instructions given
- [ ] minimal docstrings (one line describing what the function does) are
included for all functions; full docstrings describing the inputs and expected
outputs should be given for non-trivial functions

**Structure**

- [ ] code should pass lint checks (`make lint`)
- [ ] any required metadata files are checked into the repository and placed
within the directory `static`
- [ ] any intermediate files that are created and stored by the module should
be placed in the directory `cache`
- [ ] final expected output files to be uploaded to the API are placed in the
`receiving` directory; output files should not be committed to the respository
- [ ] all options and API keys are passed through the file `params.json`
- [ ] template parameter file (`params.json.template`) is checked into the
code; no personal (i.e., usernames) or private (i.e., API keys) information is
included in this template file

**Testing**

- [ ] module can be installed in a new virtual environment (`make install`)
- [ ] reasonably high level of unit test coverage covering all of the main logic
of the code (e.g., missing coverage for raised errors that do not currently seem
possible to reach are okay; missing coverage for options that will be needed are
not)
- [ ] all unit tests run without errors (`make test`)
- [ ] indicator directory has been added to GitHub CI
(`covidcast-indicators/.github/workflows/python-ci.yml`)
