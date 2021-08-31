library(dplyr)
library(readr)

context("Testing functions for exporting data for ingestion to the API")

test_that("testing write_data_api command", {
  tdir <- tempfile()

  test_data <- tibble(
    day = as.Date(c("2020-05-10", "2020-05-10", "2020-05-10", "2020-05-20")),
    geo_id = c("MA", "MA", "MA", "MA"),
    val = c(1, 2, 3, 4),
    se = c(10, 20, 30, 40),
    sample_size = c(100, 200, 300, 400),
    effective_sample_size = c(100, 200, 300, 400)
  )

  write_data_api(test_data, params = list(export_dir = tdir), "state", "test")
  expect_setequal(!!dir(tdir), c("20200510_state_test.csv", "20200520_state_test.csv"))

  df <- read_csv(file.path(tdir, "20200510_state_test.csv"))
  expect_equivalent(df, test_data[seq(1, 3), -1L])

  df <- read_csv(file.path(tdir, "20200520_state_test.csv"))
  expect_equivalent(df, test_data[4L, -1L])
})
