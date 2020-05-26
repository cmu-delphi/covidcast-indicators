library(delphiFacebook)
library(dplyr)
library(lubridate)
library(readr)

context("Testing creating of individual response output data for external sharing")

test_that("testing write_individual command", {
  tdir <- tempfile()

  test_data <- tibble(var1 = LETTERS, var2 = letters, token = LETTERS)

  write_individual(test_data, params = list(
    individual_dir = tdir,
    start_time = make_datetime(2020, 01, 01),
    end_time = make_datetime(2020, 02, 01)
  ))
  expect_setequal(
    dir(tdir),
    "cvid_responses_16_00_2019_12_31_-_16_00_2020_01_31_for_weights_2019_12_31.csv"
  )

  df <- read_csv(file.path(
    tdir,
    "cvid_responses_16_00_2019_12_31_-_16_00_2020_01_31_for_weights_2019_12_31.csv"
  ))
  expect_equivalent(df, test_data[, -3L])

})
