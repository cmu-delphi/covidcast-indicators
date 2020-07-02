### Integration test that compares the individual file output of the full
### pipeline. By "individual file" we mean the files intended for sharing with
### research partners, containing full individual responses.
###
### See `test-gold.R` for details on how the test data is generated.

library(delphiFacebook)
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
  SKIP_COLS <- c("StartDatetime", # old was UTC-7, new is UTC
                 "EndDatetime" # old was UTC-7, new is UTC
                 )

  for (f in expected_files) {
    test_df <- read.csv(test_path("individual_full", f))
    gold_df <- read.csv(test_path("gold_individual", f))

    ## the gold pipeline accidentally provides duplicate rows when there are
    ## duplicate weights. This should not be possible in the real pipeline,
    ## since an external step removes duplicate weights; but it is possible on
    ## the synthetic data. Take the *first* such row.
    dup_idx <- duplicated(gold_df$StartDatetime)
    gold_df <- gold_df[!dup_idx,]

    ## unfortunately, since StartDatetimes in synthetic data are drawn with
    ## replacement from real data, we must remove dupes from the test data.
    ## They're real dupes, but were removed from the gold data as a false
    ## positive.
    dup_idx <- duplicated(test_df$StartDatetime)
    test_df <- test_df[!dup_idx,]

    expect_equal(nrow(gold_df), nrow(test_df))

    for (col in names(test_df)) {
      if (!(col %in% SKIP_COLS)) {
        expect_equal(gold_df[[!!col]], test_df[[!!col]],
                     tolerance = 0.0001)
      }
    }
  }
})
