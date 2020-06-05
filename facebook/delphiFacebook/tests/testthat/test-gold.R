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

  for (f in expected_files)
  {
    geos <- function(dir, filename) {
      read_csv(file.path(dir, filename))$geo_id
    }

    expect_setequal(geos("receiving_full", !!f), geos("gold", !!f))
  }

})

test_that("testing files contain the same val", {
  for (f in expected_files)
  {
    vals <- function(dir, filename) {
      arrange(read.csv(file.path(dir, filename)), geo_id)$val
    }

    expect_equal(vals("receiving_full", !!f), vals("gold", !!f), tolerance = 0.0001)
  }

})

test_that("testing files contain the same se", {

  for (f in expected_files)
  {
    ses <- function(dir, filename) {
      arrange(read.csv(file.path(dir, filename)), geo_id)$se
    }

    expect_equal(ses("receiving_full", !!f), ses("gold", !!f), tolerance = 0.001)
  }

})


test_that("testing files contain the same sample size", {

  for (f in expected_files)
  {
    sample_sizes <- function(dir, filename) {
      arrange(read.csv(file.path(dir, filename)), geo_id)$sample_size
    }

    expect_equal(sample_sizes("receiving_full", !!f),
                 sample_sizes("gold", !!f), tolerance = 0.0001)
  }

})

test_that("testing files contain the same effective sample size", {

  for (f in expected_files)
  {
    effective_sample_sizes <- function(dir, filename) {
      arrange(read.csv(file.path(dir, filename)), geo_id)$effective_sample_size
    }

    expect_equal(effective_sample_sizes("receiving_full", !!f),
                 effective_sample_sizes("gold", !!f), tolerance = 0.0001)
  }

})
