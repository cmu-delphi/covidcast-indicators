### Code Review

A code review of this module should include a careful look at the code and the
output. To assist in the process, but certainly not in replace of it, please
check the following items:

- [ ] README.md file is completely filled out and currently accurate; possible
to load and test the code using only the instructions given
- [ ] any required metadata files are checked into the repository and placed
within the directory `static`
- [ ] all options and API keys are passed through the file `params.json`
- [ ] no private information has been commited to the repository
- [ ] module can be installed in a new virtual environment
- [ ] running module produces expected output files in `receiving`
- [ ] pylint run over the module produces minimal warnings; warnings that do
exist do not effect the desired outcome of the code
- [ ] all unit tests run without errors
- [ ] unit tests exist to check out the output of all intermediate values from
module functions
- [ ] reasonably high level of unit test coverage; missing coverage does not
correspond to required code-paths (e.g., missing coverage for raised errors that
do not currently seem possible to reach are OK; missing coverage for options
that will be needed are not)
- [ ] minimal docstrings are included for all functions; full docstrings
describing the inputs and expected outputs should be given for non-trivial
functions
