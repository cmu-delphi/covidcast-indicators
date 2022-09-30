### Integration test that compares the individual file output of the full
### pipeline. By "individual file" we mean the files intended for sharing with
### research partners, containing full individual responses.
###
### See `test-gold.R` for details on how the test data is generated.

library(dplyr)
library(readr)
library(stringi)
library(testthat)

context("Testing individual output against reference implementation")

expected_files <- dir(test_path("gold_individual"))

test_that("individual output files exist", {
  expect_true(all(file.exists(test_path("individual_full", expected_files))))
})

test_that("contents are the same", {
  for (f in expected_files) {
    test_df <- read.csv(test_path("individual_full", f))
    gold_df <- read.csv(test_path("gold_individual", f)) %>%
      mutate(weight_wp = weight, weight_wf = weight)

    expect_equal(nrow(gold_df), nrow(test_df))

    for (col in names(gold_df)) {
      expect_equal(gold_df[[!!col]], test_df[[!!col]],
                   tolerance = 0.0001)
    }
  }
})
