## Helper functions to relativize paths to the testing directory, so tests can
## be run via R CMD CHECK and do not depend on the current working directory
## being tests/testthat/.

library(testthat)

relativize_params <- function(params) {
    params$static_dir <- test_path(params$static_dir)
    params$export_dir <- test_path(params$export_dir)
    params$cache_dir <- test_path(params$cache_dir)
    params$archive_dir <- test_path(params$archive_dir)
    params$individual_in_dir <- test_path(params$individual_in_dir)
    params$weights_in_dir <- test_path(params$weights_in_dir)
    params$weights_out_dir <- test_path(params$weights_out_dir)
    params$input_dir <- test_path(params$input_dir)

    return(params)
}
