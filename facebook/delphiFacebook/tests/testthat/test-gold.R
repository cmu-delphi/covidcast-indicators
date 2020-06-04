### Integration test that compares the output of the full pipeline against saved
### "gold" reference output. The pipeline is run on the test data by
### `setup-run.R`, automatically run by testthat as part of the test harness.
### The test data is removed by `teardown-run.R` to clean up.
###
### This test file specifically tests the configuration in `params-full.json`,
### which loads the synthetic data in `input/full_synthetic.csv`.

library(delphiFacebook)
library(dplyr)
library(readr)
library(stringi)
library(testthat)

context("Testing against reference implementation")

geo_levels <- c("state", "county", "hrr", "msa")
dates <- c("20200508", "20200509", "20200510", "20200511", "20200512")
metrics <- c("raw_cli", "raw_ili", "raw_wcli", "raw_wili",
             "smoothed_cli", "smoothed_ili", "smoothed_wcli", "smoothed_wili")

geo_levels <- c("state", "msa", "hrr")
metrics <- c("raw_cli", "raw_ili")

grid <- expand.grid(
 geo_levels = geo_levels, dates = dates, metrics = metrics, stringsAsFactors = FALSE
)
expected_files <- sprintf("%s_%s_%s.csv", grid$dates, grid$geo_levels, grid$metrics)

test_that("testing existence of csv files", {
  expect_true(all(file.exists(file.path("gold", expected_files))))
  expect_true(all(file.exists(file.path("receiving_full", expected_files))))
})

test_that("testing files contain the same geo", {

  for (i in seq_along(expected_files))
  {
    test <- read_csv(file.path("receiving_full", expected_files[i]))
    gold <- read_csv(file.path("gold", expected_files[i]))

    expect_setequal(test$geo_id, gold$geo_id)
  }

})

test_that("testing files contain the same val", {
  for (i in seq_along(expected_files))
  {
    test <- read_csv(file.path("receiving_full", expected_files[i]))
    gold <- read_csv(file.path("gold", expected_files[i]))

    test <- arrange(test, geo_id)
    gold <- arrange(gold, geo_id)

    expect_equal(test$val, gold$val, tolerance = 0.0001,
                 info = expected_files[i])
  }

})

test_that("testing files contain the same se", {

  for (i in seq_along(expected_files))
  {
    test <- read_csv(file.path("receiving_full", expected_files[i]))
    gold <- read_csv(file.path("gold", expected_files[i]))

    test <- arrange(test, geo_id)
    gold <- arrange(gold, geo_id)

    expect_equal(test$se, gold$se, tolerance = 0.001,
                 info = expected_files[i])
  }

})


test_that("testing files contain the same sample size", {

  for (i in seq_along(expected_files))
  {
    test <- read_csv(file.path("receiving_full", expected_files[i]))
    gold <- read_csv(file.path("gold", expected_files[i]))

    test <- arrange(test, geo_id)
    gold <- arrange(gold, geo_id)

    expect_equal(test$sample_size, gold$sample_size,
                 tolerance = 0.0001,
                 info = expected_files[i])
  }

})
