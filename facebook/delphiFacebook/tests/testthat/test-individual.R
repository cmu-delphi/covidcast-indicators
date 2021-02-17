library(dplyr)
library(lubridate)
library(readr)

context("Testing creation of individual response output data for external sharing")

test_that("testing write_individual command", {
  tdir <- tempfile()

  test_data <- tibble(var1 = LETTERS, var2 = letters, weight = 1,
                      token = LETTERS, Date = "2020-01-04",
                      geo_id = LETTERS)

  write_individual(test_data, params = list(
    individual_dir = tdir,
    end_date = as.Date("2020-01-05")
  ))
  expect_setequal(
    !!dir(tdir),
    "cvid_responses_2020_01_04_recordedby_2020_01_05.csv"
  )

  df <- read_csv(file.path(
    tdir,
    "cvid_responses_2020_01_04_recordedby_2020_01_05.csv"
  ))

  # skip token, Date columns
  expect_equivalent(df, test_data[, -c(4, 5)])

})
