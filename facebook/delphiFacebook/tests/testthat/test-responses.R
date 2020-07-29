library(delphiFacebook)
library(testthat)

# These tests cover the backfill and archiving behavior described in the
# indicator README, which is quite involved.

context("Test response merging and archiving functions")

test_that("first response with a token is used", {
  input_data <- data.frame(
    StartDate = as.Date(c("2020-02-01", "2020-02-02", "2020-01-05", "2020-06-01")),
    token = c("A", "B", "C", "D")
  )

  archive <- list(
    input_data = data.frame(
      StartDate = as.Date(c("2020-09-01", "2020-01-01", "2020-01-05")),
      token = c("A", "B", "C")
    )
  )

  out <- merge_responses(input_data, archive)
  expected <- data.frame(
    StartDate = as.Date(c("2020-01-01", "2020-01-05", "2020-02-01", "2020-06-01")),
    token = c("B", "C", "A", "D")
  )

  # need to do this to avoid checking the row names
  expect_equal(out$StartDate, expected$StartDate)
  expect_equal(out$token, expected$token)

  # However, if a token was previously seen long before it was used in the
  # archive, we should not include it in the output. If it was previously seen
  # *after* it occurred in the new data, we should keep the new data.
  archive$seen_tokens <- data.frame(
    token = c("A", "B", "C"),
    start_dt = as.Date(c("2019-01-01", "2020-01-15", "2020-02-01"))
  )

  # yes, there are StartDate and start_dt columns and we need both...
  input_data$start_dt <- input_data$StartDate
  archive$input_data$start_dt <- archive$input_data$StartDate

  out <- merge_responses(input_data, archive)
  expected <- data.frame(
    StartDate = as.Date(c("2020-01-01", "2020-01-05", "2020-06-01")),
    token = c("B", "C", "D")
  )

  expect_equal(out$StartDate, expected$StartDate)
  expect_equal(out$token, expected$token)
})
