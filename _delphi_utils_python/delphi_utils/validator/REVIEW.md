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

- [ ] code should use 4 spaces for indentation; other style decisions are
flexible, but be consistent within a module
- [ ] any required metadata files are checked into the repository and placed
within the directory `static`
- [ ] any intermediate files that are created and stored by the module should
be placed in the directory `cache`
- [ ] all options and API keys are passed through the file `params.json`
- [ ] template parameter file (`params.json.template`) is checked into the
code; no personal (i.e., usernames) or private (i.e., API keys) information is
included in this template file

**Testing**

- [ ] module can be installed in a new virtual environment
- [ ] pylint with the default `.pylint` settings run over the module produces
minimal warnings; warnings that do exist have been confirmed as false positives
- [ ] reasonably high level of unit test coverage covering all of the main logic
of the code (e.g., missing coverage for raised errors that do not currently seem
possible to reach are okay; missing coverage for options that will be needed are
not)
- [ ] all unit tests run without errors
