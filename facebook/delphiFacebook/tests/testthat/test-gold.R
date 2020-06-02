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

<<<<<<< HEAD
geo_levels <- c("state", "msa", "hrr")
=======
geo_levels <- c("state")
>>>>>>> 6f14e10424257f68db6c8f06fda67a001ddb3c4c
metrics <- c("raw_cli", "raw_ili")

grid <- expand.grid(
 geo_levels = geo_levels, dates = dates, metrics = metrics, stringsAsFactors = FALSE
)
expected_files <- sprintf("%s_%s_%s.csv", grid$dates, grid$geo_levels, grid$metrics)

test_that("testing existance of csv files", {
<<<<<<< HEAD
  expect_true(all(file.exists(file.path("gold", expected_files))))
  expect_true(all(file.exists(file.path("receiving_full", expected_files))))
=======
  # expect_true(all(file.exists(file.path("receiving_gold", expected_files))))
  # expect_true(all(file.exists(file.path("receiving_full", expected_files))))
>>>>>>> 6f14e10424257f68db6c8f06fda67a001ddb3c4c
})

test_that("testing files contain the same geo", {

  for (i in seq_along(expected_files))
  {
<<<<<<< HEAD
    test <- read_csv(file.path("receiving_full", expected_files[i]))
    gold <- read_csv(file.path("gold", expected_files[i]))

    expect_setequal(test$geo_id, gold$geo_id)
=======
    # test <- read_csv(file.path("receiving_full", expected_files[i]))
    # gold <- read_csv(file.path("receiving_gold", expected_files[i]))

    # expect_setequal(test$geo_id, gold$geo_id)
>>>>>>> 6f14e10424257f68db6c8f06fda67a001ddb3c4c
  }

})

test_that("testing files contain the same val", {

  for (i in seq_along(expected_files))
  {
<<<<<<< HEAD
    test <- read_csv(file.path("receiving_full", expected_files[i]))
    gold <- read_csv(file.path("gold", expected_files[i]))

    test <- arrange(test, geo_id)
     gold <- arrange(gold, geo_id)

    expect_true(max(abs(test$val - gold$val)) < 0.0001, info = expected_files[i])
=======
    # test <- read_csv(file.path("receiving_full", expected_files[i]))
    # gold <- read_csv(file.path("receiving_gold", expected_files[i]))

    # test <- arrange(test, geo_id)
    # gold <- arrange(gold, geo_id)

    # expect_true(max(abs(test$val - gold$val)) < 0.001, info = expected_files[i])
>>>>>>> 6f14e10424257f68db6c8f06fda67a001ddb3c4c
  }

})

test_that("testing files contain the same se", {

  for (i in seq_along(expected_files))
  {
<<<<<<< HEAD
    test <- read_csv(file.path("receiving_full", expected_files[i]))
    gold <- read_csv(file.path("gold", expected_files[i]))

    test <- arrange(test, geo_id)
    gold <- arrange(gold, geo_id)

    expect_true(max(abs(test$se - gold$se)) < 0.001, info = expected_files[i])
=======
    # test <- read_csv(file.path("receiving_full", expected_files[i]))
    # gold <- read_csv(file.path("receiving_gold", expected_files[i]))

    # test <- arrange(test, geo_id)
    # gold <- arrange(gold, geo_id)

    # expect_true(max(abs(test$se - gold$se)) < 0.001, info = expected_files[i])
>>>>>>> 6f14e10424257f68db6c8f06fda67a001ddb3c4c

  }

})


test_that("testing files contain the same sample size", {

  for (i in seq_along(expected_files))
  {
<<<<<<< HEAD
    test <- read_csv(file.path("receiving_full", expected_files[i]))
    gold <- read_csv(file.path("gold", expected_files[i]))

    test <- arrange(test, geo_id)
    gold <- arrange(gold, geo_id)

    expect_true(
      max(abs(test$sample_size - gold$sample_size)) < 0.0001,
      info = expected_files[i]
    )
=======
    # test <- read_csv(file.path("receiving_full", expected_files[i]))
    # gold <- read_csv(file.path("receiving_gold", expected_files[i]))
    #
    # test <- arrange(test, geo_id)
    # gold <- arrange(gold, geo_id)

    # expect_true(
    #   max(abs(test$sample_size - gold$sample_size)) < 0.01,
    #   info = expected_files[i]
    # )
>>>>>>> 6f14e10424257f68db6c8f06fda67a001ddb3c4c
  }

})
