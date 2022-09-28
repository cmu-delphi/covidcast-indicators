## Helper functions to relativize paths to the testing directory, so tests can
## be run via R CMD CHECK and do not depend on the current working directory
## being tests/testthat/.

library(testthat)

relativize_params <- function(params) {
    params$export_dir <- test_path(params$export_dir)
    params$cache_dir <- test_path(params$cache_dir)
    params$input_dir <- test_path(params$input_dir)

    return(params)
}
