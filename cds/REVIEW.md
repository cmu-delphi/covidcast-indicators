## Code Review (Python)

A code review of this module should include a careful look at the code and the
output. To assist in the process, but certainly not in replace of it, please
check the following items.

**Documentation**

- [x] the README.md file template is filled out and currently accurate; it is
possible to load and test the code using only the instructions given
- [x] minimal docstrings (one line describing what the function does) are
included for all functions; full docstrings describing the inputs and expected
outputs should be given for non-trivial functions

**Structure**

- [x] code should use 4 spaces for indentation; other style decisions are
flexible, but be consistent within a module
- [x] any required metadata files are checked into the repository and placed
within the directory `static`
- [x] any intermediate files that are created and stored by the module should
be placed in the directory `cache`
- [x] final expected output files to be uploaded to the API are placed in the
`receiving` directory; output files should not be committed to the respository
- [x] all options and API keys are passed through the file `params.json`
- [x] template parameter file (`params.json.template`) is checked into the
code; no personal (i.e., usernames) or private (i.e., API keys) information is
included in this template file

**Testing**

- [x] module can be installed in a new virtual environment
- [x] pylint with the default `.pylint` settings run over the module produces
minimal warnings; warnings that do exist have been confirmed as false positives
- [x] reasonably high level of unit test coverage covering all of the main logic
of the code (e.g., missing coverage for raised errors that do not currently seem
possible to reach are okay; missing coverage for options that will be needed are
not)
- [x] all unit tests run without errors
