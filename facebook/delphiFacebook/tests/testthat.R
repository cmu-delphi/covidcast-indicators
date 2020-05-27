library(testthat)
library(delphiFacebook)

test_check("delphiFacebook", stop_on_warning = FALSE)
test_file(file.path("testthat", "teardown-run.R"))

