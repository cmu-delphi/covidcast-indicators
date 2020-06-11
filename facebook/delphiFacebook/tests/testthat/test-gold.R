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

## TODO County left out here, because megacounties are not done
geo_levels <- c("state", "hrr", "msa")
dates <- c("20200508", "20200509", "20200510", "20200511", "20200512")

## Smoothed signals are not included because we deliberately mismatch the old
## signals. The old pipeline did not produce weighted community estimates, so we
## can't check those here.
metrics <- c("raw_cli", "raw_ili", "raw_wcli", "raw_wili",
             "raw_nohh_cmnty_cli", "raw_hh_cmnty_cli")

grid <- expand.grid(
 geo_levels = geo_levels, dates = dates, metrics = metrics, stringsAsFactors = FALSE
)
expected_files <- sprintf("%s_%s_%s.csv", grid$dates, grid$geo_levels, grid$metrics)

test_that("testing existence of csv files", {
  expect_true(all(file.exists(file.path("gold", expected_files))))
  expect_true(all(file.exists(file.path("receiving_full", expected_files))))
})

test_that("testing files contain the same geo", {

  geos <- function(dir, filename) {
    g <- read_csv(file.path(dir, filename))$geo_id

    ## Gross hack: All geo IDs are numeric except states, which are
    ## two-character strings. However, sometimes the numeric geo-IDs are
    ## zero-padded and sometimes they are not; make them consistent by turning
    ## them into numbers so we can match across pipelines.
    if (is.character(g) && all(nchar(g) > 2)) {
      return(as.numeric(g))
    }
    return(g)
  }

  for (f in expected_files)
  {
    expect_setequal(geos("receiving_full", !!f), geos("gold", !!f))
  }

})

test_that("testing files contain the same val", {

  vals <- function(dir, filename) {
    arrange(read.csv(file.path(dir, filename)), geo_id)$val
  }

  for (f in expected_files)
  {
    expect_equal(vals("receiving_full", !!f), vals("gold", !!f), tolerance = 0.0001)
  }

})

test_that("testing files contain the same se", {
  ses <- function(dir, filename) {
    arrange(read.csv(file.path(dir, filename)), geo_id)$se
  }

  for (f in expected_files)
  {

    expect_equal(ses("receiving_full", !!f), ses("gold", !!f), tolerance = 0.0001)
  }

})


test_that("testing files contain the same sample size", {

  sample_sizes <- function(dir, filename) {
    arrange(read.csv(file.path(dir, filename)), geo_id)$sample_size
  }

  for (f in expected_files)
  {
    expect_equal(sample_sizes("receiving_full", !!f),
                 sample_sizes("gold", !!f), tolerance = 0.0001)
  }

})

test_that("testing files contain the same effective sample size", {

  effective_sample_sizes <- function(dir, filename) {
    arrange(read.csv(file.path(dir, filename)), geo_id)$effective_sample_size
  }

  for (f in expected_files)
  {
    ## Community gold files don't have effective sample sizes; this is wasteful
    ## but easy to check
    if ("effective_sample_size" %in% names(read.csv(file.path("gold", f)))) {
      expect_equal(effective_sample_sizes("receiving_full", !!f),
                   effective_sample_sizes("gold", !!f), tolerance = 0.0001)
    }
  }

})
