## Code Review (R)

A code review of this package should include a careful look at the code and the
output. To assist in the process, but certainly not in replace of it, please
check the following items.

**Documentation**

- [ ] the README.md file template is filled out and currently accurate; it is
possible to load and test the code using only the instructions given
- [ ] documentation is written using **roxygen2** style comments; for exported functions,
sufficent documentation should be included to pass the automatic R checks; for internal
functions, sufficent documentation should be given to explain what the goal of each
function is

**Structure**

- [ ] code is wrapped into a package named according to the pattern "delphiPackageName"
- [ ] code should use 2 spaces for indentation; if you make extensive use of the "tidyverse"
set of packages, you should attempt to conform to tidyverse-style guidelines
- [ ] any required metadata files are checked into the repository and placed
within the directory `static`
- [ ] final expected output files to be uploaded to the API are placed in the
`receiving` directory; output files should not be committed to the respository
- [ ] all options and API keys are passed through the file `params.json`
- [ ] template parameter file (`params.json.template`) is checked into the
code; no personal (i.e., usernames) or private (i.e., API keys) information is
included in this template file

**Testing**

- [ ] the R package can be installed
- [ ] R CMD CHECK can be run on the package with minimal notes and warnings; warnings that
do exist have been confirmed as false positives
- [ ] reasonably high level of unit test coverage covering all of the main logic
of the code (e.g., missing coverage for raised errors that do not currently seem
possible to reach are okay; missing coverage for options that will be needed are
not)
- [ ] all unit tests run without errors
