library(dplyr)
library(lubridate)
library(readr)

context("Testing functions for loading and saving survey weights")

test_that("testing write_cid command", {

  fake_data <- tibble(
    token = c("DSFIJBjAexoQjDStr", "mGDsqbweUYzFnmZUH", "zocUNXYISDyYcVIQn"),
    Date = c("2020-01-04", "2020-07-25", "2024-03-04"),
    other = runif(3)
  )
  params <- list(
    start_date = "2020-01-02",
    end_date = "2025-06-07",
    start_time = ymd_hm("2020-01-02 03:04", tz = "America/Los_Angeles"),
    end_time = ymd_hm("2025-06-07 08:09", tz = "America/Los_Angeles"),
    weights_out_dir = tempfile()
  )

  write_cid(fake_data, "fake", params)
  exp_file <- "cvid_cids_fake_response_03_04_2020_01_02_-_08_09_2025_06_07.csv"
  expect_true(file.exists(file.path(params$weights_out_dir, exp_file)))

  df <- read_csv(file.path(params$weights_out_dir, exp_file), col_names = "token")
  expect_setequal(df$token, fake_data$token)
})

test_that("testing add_weights command", {

  expected_weights <- c(547.261991770938, 741.556362016127, 392.85944076255)
  fake_data <- tibble(
    token = c("DSFIJBjAexoQjDStr", "mGDsqbweUYzFnmZUH", "zocUNXYISDyYcVIQn")
  )
  fake_data_w <- add_weights(fake_data, list(weights_in_dir = "weights_in"))$df

  expect_true(max(abs(expected_weights - fake_data_w$weight)) < 1e-7)

})
