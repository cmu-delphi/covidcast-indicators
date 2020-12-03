library(testthat)

# These tests cover the backfill and archiving behavior described in the
# indicator README, which is quite involved.

context("Test response merging and archiving functions")

test_that("first response with a token is used", {
  input_data <- data.frame(
    StartDate = as.Date(c("2020-02-01", "2020-02-02", "2020-01-05", "2020-06-01")),
    token = c("A", "B", "C", "D"),
    stringsAsFactors = FALSE
  )

  archive <- list(
    input_data = data.frame(
      StartDate = as.Date(c("2020-09-01", "2020-01-01", "2020-01-05")),
      token = c("A", "B", "C"),
      stringsAsFactors = FALSE
    )
  )

  out <- merge_responses(input_data, archive)
  expected <- data.frame(
    StartDate = as.Date(c("2020-01-01", "2020-01-05", "2020-02-01", "2020-06-01")),
    token = c("B", "C", "A", "D"),
    stringsAsFactors = FALSE
  )

  # need to do this to avoid checking the row names
  expect_equal(out$StartDate, expected$StartDate)
  expect_equal(out$token, expected$token)

  # However, if a token was previously seen long before it was used in the
  # archive, we should not include it in the output. If it was previously seen
  # *after* it occurred in the new data, we should keep the new data.
  archive$seen_tokens <- data.frame(
    token = c("A", "B", "C"),
    start_dt = as.Date(c("2019-01-01", "2020-01-15", "2020-02-01")),
    stringsAsFactors = FALSE
  )

  # yes, there are StartDate and start_dt columns and we need both...
  input_data$start_dt <- input_data$StartDate
  archive$input_data$start_dt <- archive$input_data$StartDate

  out <- merge_responses(input_data, archive)
  expected <- data.frame(
    StartDate = as.Date(c("2020-01-01", "2020-01-05", "2020-06-01")),
    token = c("B", "C", "D"),
    stringsAsFactors = FALSE
  )

  expect_equal(!!out$StartDate, !!expected$StartDate)
  expect_equal(!!out$token, !!expected$token)
})

test_that("in case of duplicates, new input takes precedence", {
  # Suppose there is an error in an input file and it gets stored in the
  # archive. (Maybe load_responses() has a bug in its calculations that we later
  # fix.) We should be able to re-run the pipeline with the new input file or
  # new calculations and have the new values take precedence over the archived
  # values.

  input_data <- data.frame(
    StartDate = as.Date(c("2020-02-01", "2020-02-02", "2020-01-05", "2020-06-01")),
    token = c("A", "B", "C", "D"),
    some_value = 1:4,
    stringsAsFactors = FALSE
  )

  archive <- list(
    input_data = data.frame(
      StartDate = as.Date(c("2020-09-01", "2020-01-01", "2020-01-05", "2020-06-01")),
      token = c("A", "B", "C", "D"),
      some_value = 5:8,
      stringsAsFactors = FALSE
    )
  )

  out <- merge_responses(input_data, archive)

  expected <- data.frame(
    StartDate = as.Date(c("2020-01-01", "2020-01-05", "2020-02-01", "2020-06-01")),
    token = c("B", "C", "A", "D"),
    some_value = c(6, 3, 1, 4),
    stringsAsFactors = FALSE
  )

  expect_equal(!!out$StartDate, !!expected$StartDate)
  expect_equal(!!out$token, !!expected$token)
  expect_equal(!!out$some_value, !!expected$some_value)
})
